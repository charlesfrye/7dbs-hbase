import 'org.apache.hadoop.hbase.client.ConnectionFactory'
import 'org.apache.hadoop.hbase.client.Put'

def jbytes( *args )
  args.map { |arg| arg.to_s.to_java_bytes }
end

connection = ConnectionFactory.createConnection( @hbase.configuration )

table = connection.getTable( TableName.valueOf("wiki" ) )

p = Put.new( *jbytes( "Home" ) )

p.addColumn( *jbytes( "text", "", "Hello world" ) )
p.addColumn( *jbytes( "revision", "author", "jimbo" ) )
p.addColumn( *jbytes( "revision", "comment", "my first edit" ) )

table.put( p )

table.close()
connection.close()
