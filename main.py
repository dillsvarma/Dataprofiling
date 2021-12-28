import pandas as pd
import pyspark.sql.functions as F
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType

if __name__ == "__main__":
    f = open('GFG.html', 'w')

    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    df = spark.read.option('inferschema', 'true').csv('sale.csv', header='true')
    rows = df.count()
    col = len(df.columns)
    colNames = spark.createDataFrame(df.columns, StringType()).toPandas().to_html(index=False, col_space="40px",
                                                                                  classes=('table', 'table-striped'))

    print('Sample Data')
    df.show(5)
    # sample = df.head(5)
    sample = df.toPandas().iloc[:5, ].to_html(index=False, col_space="40px", classes=('table', 'table-striped'))
    dataTypes = pd.DataFrame(df.dtypes).to_html(index=False, col_space="40px", classes=('table', 'table-striped'))

    # spark.createDataFrame(df.dtypes, StringType()).toPandas().to_html(index=False, col_space="40px", classes=('table', 'table-striped'))
    # df.select([count(when(isnan(c), c)).alias(c) for c in df.columns]).show()
    # df.select([count(when(col(c).isNull(c), c)).alias(c) for c in df.columns]).show()
    nuLl = df.select([F.count(F.when(F.isnan(c) | F.isnull(c), c)).alias(c) for (c, c_type) in df.dtypes if
                      c_type in ('timestamp', 'string', 'date', 'int', 'double')]).toPandas().to_html(index=False,
                                                                                                      col_space="40px",
                                                                                                      classes=('table',
                                                                                                               'table-striped'))
    r = nuLl
    expression = [F.countDistinct(c).alias(c) for c in df.columns]
    # exp= df.select([F.countDistinct(c).alias(c) for c in df.columns])
    exp = df.select([F.countDistinct(c).alias(c) for c in df.columns]).toPandas().to_html(index=False, col_space="40px",
                                                                                          classes=(
                                                                                          'table', 'table-striped'))
    # expression= df.select(*expression).show()

    col50 = df.select([F.count(F.when(F.length(c) > 50, c)).alias(c) for c in df.columns]).toPandas().to_html(
        index=False, col_space="40px", classes=('table', 'table-striped'))

    dup = df.groupby(['Invoice ID', 'Product line', 'Total']).count().where('count > 1').sort('count', ascending=False)
    dup = pd.DataFrame(dup.toPandas()).to_html(index=False, col_space="40px", classes=('table', 'table-striped'))

    # if df.count() > df.dropDuplicates(df.columns).count():
    #   raise ValueError('Data has duplicates')

    ##############################

    #   Print output to HTML file

    ##############################

    html_template = """<html>
   <head>
      <title>Title</title>
      <head>
         <link rel="stylesheet" type="text/css" href="https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css">
         <link rel="stylesheet" type="text/css" href="custom.css">
         <link href="https://fonts.googleapis.com/css?family=Raleway:100,200,400,500,600" rel="stylesheet" type="text/css">
   </head>
   </head>
   <body><div class="container" style="padding-left:100px">
         <div class="row">
         <div class="col-xs-12 col-sm-4 col-md-3 col-lg-2">
                           <h2>Data Profiling for sale.csv</h2>
                        <br><br>
                        <div class="icon-section">
                           <i class="fa fa-table" aria-hidden="true"></i><br>
                           <large><b>Total Row Count<b></large>
                           <p>{rows}</p>
                        </div> <br><br>
                        <div class="icon-section">
                           <i class="fa fa-columns" aria-hidden="true"></i><br>
                           <large><b>Total Columns<b></large>
                           <p>{col}</p>
                        </div>
                                                <br><br>

                     </div>
            <div class="col-xs-12 col-sm-4 col-md-3 col-lg-2">
               
            </div>
         </div>
         <div class="row">
            <div class="col-xs-12 col-sm-4 col-md-3 col-lg-2">
                  <div class="dashbord dashbord">Column Names</div>
                     <p>{colNames}</p>
                  <h4>Sample Data</h4>
                  <p>{sample}
                  <p/>
                  <h4>Data types</h4>
                  <p>{dataTypes}
                  <p/>
                  <h4>Distinct Rows</h4>
                  <p>{exp}
                  <p/>
                  <h4>Column Value >50 Chars</h4>
                  <p>{col50}
                  <p/>
                  <h4>Count Duplicate records</h4>
                  <p>{dup}
                  <p/>
                  <h4>Count Null records</h4>
                  <p>{nuLl}
                  <p/>
                  <br><br>
                  <p></p>
               
            </div>
         </div>
         <div class="row">
            <div class="col-xs-12 col-sm-4 col-md-3 col-lg-2"><span>@thesedeks. all rights reserved</span></div>
         </div>
      </div>
      <div class="container">
      </div>
   </body>
</html>
    """.format(colNames=colNames, nuLl=nuLl, col50=col50, sample=sample, r=r, exp=exp, col=col, rows=rows,
               dataTypes=dataTypes, dup=dup)
    html = df.toPandas().iloc[:5, ].to_html(index=False, col_space="40px", classes=('table', 'table-striped', 'isi'))

    # writing the code into the file
    f.write(html_template)

    # close the file
    f.close()
