import 'org.apache.hadoop.hbase.client.ConnectionFactory'
import 'org.apache.hadoop.hbase.client.Put'
import 'org.apache.hadoop.hbase.client.Scan'
import 'org.apache.hadoop.hbase.util.Bytes'

def jbytes(*args)
  return args.map { |arg| arg.to_s.to_java_bytes }
end

connection = ConnectionFactory.createConnection( @hbase.configuration )

wiki_table = connection.getTable(TableName.valueOf('wiki'))
links_mutator = connection.getBufferedMutator( TableName.valueOf('links') )

scanner = wiki_table.getScanner(Scan.new)

linkpattern = /\[\[([^\[\]\|\:\#][^\[\]\|:]*)(?:\|([^\[\]\|]+))?\]\]/
count = 0
while (result = scanner.next())
  title = Bytes.toString(result.getRow())
  text = Bytes.toString(result.getValue(*jbytes('text', '')))
  if text
    put_to = nil
    text.scan(linkpattern) do |target, label|
      unless put_to
        put_to = Put.new(*jbytes(title))
      end
      target.strip!
      target.capitalize!
      label = '' unless label
      label.strip!
      if !(target.empty?)
        put_to.addColumn(*jbytes("to", target, label))
        put_from = Put.new(*jbytes(target))
        put_from.addColumn(*jbytes("from", title, label))
        links_mutator.mutate(put_from)
      end
    end
    if put_to
      if put_to.toMap["totalColumns"] > 0
        links_mutator.mutate(put_to)
      end
    end
  end

  count += 1
  puts "#{count} pages processed (#{title})" if count % 500 == 0

end

exit
