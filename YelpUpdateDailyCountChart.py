# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 17:44:52 2016

@author: Gretel_MacAir
"""

# %% import libs

import sframe
import datetime as dt
import gviz_api

# %% load the data from csv into SFrame

dsf = sframe.SFrame.read_csv('YelpCountPerDay.csv', verbose=False)

# %% convert to real date

dsf['date'] = dsf['date'].apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y'))

# %% fill in gaps in the date with 0

min = dsf['date'].min()
max = dsf['date'].max()
r = (max - min).days + 2
d = min - dt.timedelta(days=1)

ts = []
for i in range(r):
    d += dt.timedelta(days=1)
    ts.append(d)

ts_dct = {}
ts_dct['date'] = ts

tsf = sframe.SFrame(ts_dct)

sf = tsf.join(dsf, how='left')

# %% html template

page_template = """
<html>
  <head>
    <script type='text/javascript' src='https://www.gstatic.com/charts/loader.js'></script>
    <script type='text/javascript'>
      google.charts.load('current', {'packages':['annotationchart']});
      google.charts.setOnLoadCallback(drawChart);
      function drawChart() {
      var json_data = new google.visualization.DataTable(%(json)s);
        var chart = new google.visualization.AnnotationChart(document.getElementById('chart_div'));
      var options = {displayAnnotations: true};
      chart.draw(json_data, options);
      }
    </script>
  </head>
  <body>
    <div id='chart_div' style='width: 400px; height: 250px;'></div>
  </body>
</html>
"""

# %% convert the sf into what is required by google charts: data_table

description = {"date": ("datetime", "date"),
               "count": ("number", "count")}
data_table = gviz_api.DataTable(description)
data_table.LoadData(sf)

# %% paste the data into the html temlate

json = data_table.ToJSon(columns_order=("date", "count"),
                         order_by="date")
html = page_template % vars()

# %% save the html page in the default webserver folder

f = open('/var/www/html/index.html', 'w')
f.write(html)
f.close()
