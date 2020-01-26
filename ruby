create_namespace 'hell'
create 'hell:customer_table' , 'cust'
for i in 0..100
     put 'hell:customer' , 'r'+i.to_s,'cust:name','name'+i.to_s
     put 'hell:customer' , 'r'+i.to_s,'cust:place','place'+i.to_s
     put 'hell:customer' , 'r'+i.to_s,'cust:number', i.to_s
end







scan 'hell:customer',{ COLUMNS => 'cust:name',LIMIT => 10,FILTER => "ValueFilter( =, 'binaryprefix:value=name91' )" }

scan 'hell:customer', { FILTER => SingleColumnValueFilter.new(Bytes.toBytes('cust'), Bytes.toBytes('name'), CompareFilter::CompareOp.valueOf('EQUAL'),BinaryComparator.new(Bytes.toBytes('name1')))}





require 'java'
require 'net/http'

import java.io.IOException
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.MasterNotRunningException
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor

log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)

config = HBaseConfiguration.create
config.set 'fs.default.name', config.get(HConstants::HBASE_DIR)

TAB = 'hell:customer'.to_java_bytes
FAM = 'cust'.to_java_bytes
ID = '1'.to_java_bytes

scan = Scan.new()
scan.setCacheBlocks(false)
scan.cachlocks = false
scan.caching = 10
scan.addColumn(FAM, ID)


#filter = SingleColumnValueFilter.new(FAM, ID, CompareFilter::CompareOp.valueOf('EQUAL'), '1'.to_java_bytes)
filter = SingleColumnValueFilter.new(Bytes.toBytes('cust'), Bytes.toBytes('name'), CompareFilter::CompareOp.valueOf('EQUAL'),BinaryComparator.new(Bytes.toBytes('name1')))
filter.setFilterIfMissing(true)

scan.setFilter(filter)


iter = nil 
table = nil
scanner = nil
while true
  begin
    table = HTable.new config, TAB
    scanner = table.getScanner(scan)    
    iter = scanner.iterator
    break
  rescue IOException => ioe
    print "Exception trying to scan #{TAB}: #{ioe}"
    sleep 1
  end
end

start = Time.at(java.util.Date.new.getTime/1000)
print "Start Time : " + start.inspect

while iter.hasNext
  result = iter.next
  delete = Delete.new result.getRow()
  delete.addColumn(FAM, 'bdate'.to_java_bytes)
  table.delete delete
end

scanner.close


ending = Time.at(java.util.Date.new.getTime/1000)
print "Start Time : " + ending.inspect




















disable 'hell:product'
drop 'hell:product'

create 'hell:product' , 'prod'
bdate='2019-01-01'
for i in 0..100
     put 'hell:product' , 'r'+i.to_s+bdate.to_s,'prod:name','name'+i.to_s
     put 'hell:product' , 'r'+i.to_s+bdate.to_s,'prod:place','place'+i.to_s
     put 'hell:product' , 'r'+i.to_s+bdate.to_s,'prod:number', i.to_s
     put 'hell:product' , 'r'+i.to_s+bdate.to_s,'prod:bdate', bdate.to_s
end

bdate='2020-01-01'
for i in 0..100
     put 'hell:product' , 'r'+i.to_s+bdate.to_s,'prod:name','name'+i.to_s
     put 'hell:product' , 'r'+i.to_s+bdate.to_s,'prod:place','place'+i.to_s
     put 'hell:product' , 'r'+i.to_s+bdate.to_s,'prod:number', i.to_s
     put 'hell:product' , 'r'+i.to_s+bdate.to_s,'prod:bdate', bdate.to_s
end





import java.io.IOException
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.MasterNotRunningException
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor

log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)

config = HBaseConfiguration.create
config.set 'fs.default.name', config.get(HConstants::HBASE_DIR)

TAB = 'hell:product'.to_java_bytes
FAM = 'prod'.to_java_bytes

scan = Scan.new()
scan.setCacheBlocks(false)
scan.addColumn(FAM,'bdate')
filter = SingleColumnValueFilter.new(Bytes.toBytes('prod'), Bytes.toBytes('bdate'), CompareFilter::CompareOp.valueOf('EQUAL'),BinaryComparator.new(Bytes.toBytes('2019-01-01')))
filter.setFilterIfMissing(true)
scan.setFilter(filter)


iter = nil 
table = nil
scanner = nil
while true
  begin
    table = HTable.new config, TAB
    scanner = table.getScanner(scan)    
    iter = scanner.iterator
    break
  end
end


while iter.hasNext
  result = iter.next
  delete = Delete.new result.getRow()
  delete.addColumn(FAM,'bdate'.to_java_bytes)
  delete.addFamily(FAM)
  table.delete delete
end







import java.io.IOException
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.MasterNotRunningException
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor

log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)

config = HBaseConfiguration.create


TAB = 'hell:product'.to_java_bytes
FAM = 'prod'.to_java_bytes

scan = Scan.new()
scan.setCacheBlocks(false)
scan.addColumn(FAM,'bdate')
filter = SingleColumnValueFilter.new(Bytes.toBytes('prod'), Bytes.toBytes('bdate'), CompareFilter::CompareOp.valueOf('EQUAL'),BinaryComparator.new(Bytes.toBytes('2020-01-01')))
filter.setFilterIfMissing(true)
scan.setFilter(filter)

iter = nil 
table = nil
scanner = nil
while true
  begin
    table = HTable.new config, TAB
    scanner = table.getScanner(scan)    
    iter = scanner.iterator
    break
  end
end

lis=[]
while iter.hasNext
  result = iter.next  
  print(Bytes.toString(result.getValue(FAM,'name'.to_java_bytes)))
  lis.push(Bytes.toString(result.getValue(FAM,'name'.to_java_bytes)))
end


