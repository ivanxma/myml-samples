# https://data.world/amitkishore/can-you-predict-products-back-order/workspace/file?filename=data.zip
#  Getting back order
#
# https://www.convertcsv.com/csv-to-sql.htm
# a good tool to convert CSV to SQL


```
mysqlsh --uri <user>@host:port --sql
\sql
source create.sql
\js
util.importTable('Kaggle_Training_Dataset_v2.csv', {"schema":"inventory", "table":"myinventory_train", "dialect":"csv-unix", "skipRows":1})

quit
```

# create test table with sample data from train
```
create table myinventory_test like myinventory_train;
insert into myinventory_test select * from myinventory_train where went_on_backorder='yes' limit 3;
insert into myinventory_test select * from myinventory_train where went_on_backorder='no' limit 7;
delete myinventory_train from myinventory_train inner join myinventory_test on  myinventory_train.sku = myinventory_test.sku;
```



