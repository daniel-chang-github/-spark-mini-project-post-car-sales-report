Spark Mini Project

With this mini project, you will exercise using Spark transformations to solve traditional MapReduce data problems. It demonstrates Spark having a significant advantage against Hadoop MapReduce framework, in both code simplicity and its in-memory processing performance, which best suit for the chain of MapReduce use cases.

Step 1. Import the data

  sc = SparkContext("local", "My Application")
  raw_rdd = sc.textFile("data/data.csv") 

Raw Data.

    ['1,I,VXIO456XLBB630221,Nissan,Altima,2003,2002-05-08,Initial sales from TechMotors',
     '2,I,INU45KIOOPA343980,Mercedes,C300,2015,2014-01-01,Sold from EuroMotors',
     '3,A,VXIO456XLBB630221,,,,2014-07-02,Head on collision',
     '4,R,VXIO456XLBB630221,,,,2014-08-05,Repair transmission',
     '5,I,VOME254OOXW344325,Mercedes,E350,2015,2014-02-01,Sold from Carmax',
     '6,R,VOME254OOXW344325,,,,2015-02-06,Wheel allignment service',
     '7,R,VXIO456XLBB630221,,,,2015-01-01,Replace right head light',
     '8,I,EXOA00341AB123456,Mercedes,SL550,2016,2015-01-01,Sold from AceCars',
     '9,A,VOME254OOXW344325,,,,2015-10-01,Side collision',
     '10,R,VOME254OOXW344325,,,,2015-09-01,Changed tires',
     '11,R,EXOA00341AB123456,,,,2015-05-01,Repair engine',
     '12,A,EXOA00341AB123456,,,,2015-05-03,Vehicle rollover',
     '13,R,VOME254OOXW344325,,,,2015-09-01,Replace passenger side door',
     '14,I,UXIA769ABCC447906,Toyota,Camery,2017,2016-05-08,Initial sales from Carmax',
     '15,R,UXIA769ABCC447906,,,,2020-01-02,Initial sales from Carmax',
     '16,A,INU45KIOOPA343980,,,,2020-05-01,Side collision']

Setp 2. Perform map operation

  vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))
  
  Result.
  
    [('VXIO456XLBB630221', ('Nissan', '2003', 'I')),
     ('INU45KIOOPA343980', ('Mercedes', '2015', 'I')),
     ('VXIO456XLBB630221', ('', '', 'A')),
     ('VXIO456XLBB630221', ('', '', 'R')),
     ('VOME254OOXW344325', ('Mercedes', '2015', 'I')),
     ('VOME254OOXW344325', ('', '', 'R')),
     ('VXIO456XLBB630221', ('', '', 'R')),
     ('EXOA00341AB123456', ('Mercedes', '2016', 'I')),
     ('VOME254OOXW344325', ('', '', 'A')),
     ('VOME254OOXW344325', ('', '', 'R')),
     ('EXOA00341AB123456', ('', '', 'R')),
     ('EXOA00341AB123456', ('', '', 'A')),
     ('VOME254OOXW344325', ('', '', 'R')),
     ('UXIA769ABCC447906', ('Toyota', '2017', 'I')),
     ('UXIA769ABCC447906', ('', '', 'R')),
     ('INU45KIOOPA343980', ('', '', 'A'))]


Step 3. Perform group aggregation to populate make and year to all the records.

  enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
  
  Result.
  
    [('Nissan', '2003', 'I'),
     ('Nissan', '2003', 'A'),
     ('Nissan', '2003', 'R'),
     ('Nissan', '2003', 'R'),
     ('Mercedes', '2015', 'I'),
     ('Mercedes', '2015', 'A'),
     ('Mercedes', '2015', 'I'),
     ('Mercedes', '2015', 'R'),
     ('Mercedes', '2015', 'A'),
     ('Mercedes', '2015', 'R'),
     ('Mercedes', '2015', 'R'),
     ('Mercedes', '2016', 'I'),
     ('Mercedes', '2016', 'R'),
     ('Mercedes', '2016', 'A'),
     ('Toyota', '2017', 'I'),
     ('Toyota', '2017', 'R')]

Step 4. Count number of occurrence for accidents for the vehicle make and year.
 
  make_kv = enhance_make.map(lambda list_val: extract_make_key_value(list_val))
  
  Result.
  
    [('Nissan-2003', 0),
     ('Nissan-2003', 1),
     ('Nissan-2003', 0),
     ('Nissan-2003', 0),
     ('Mercedes-2015', 0),
     ('Mercedes-2015', 1),
     ('Mercedes-2015', 0),
     ('Mercedes-2015', 0),
     ('Mercedes-2015', 1),
     ('Mercedes-2015', 0),
     ('Mercedes-2015', 0),
     ('Mercedes-2016', 0),
     ('Mercedes-2016', 0),
     ('Mercedes-2016', 1),
     ('Toyota-2017', 0),
     ('Toyota-2017', 0)]

 Step 5. Aggregate the key and count the number of records in total per key.
 
  make_kv.reduceByKey(add).collect()
 
    [('Nissan-2003', 1),
    ('Mercedes-2015', 2),
    ('Mercedes-2016', 1),
    ('Toyota-2017', 0)]
 
 
  
