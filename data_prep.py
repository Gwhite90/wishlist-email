import pandas as pd
import numpy as np
import time
import smtplib

import yaml
from datetime import datetime, timedelta
from datetime import timedelta
import pymysql

import smtplib

import email
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders
import os


local_production = 'Local_Production'
Server_Production = 'Server_Production'
Local_SCV = 'Local_SCV'
Server_SCV = 'Server_SCV'
Local_Search = 'Local_Search'
Server_Search = 'Server_Search'

#########################################################
#local
#base_dir = '/Users/gary.white/Google Drive/datasciencescripts/'
#db1 = local_production
#db2 = Local_SCV
#db3 = Local_Search
#server
base_dir = '/Users/gary.white/Google Drive/datasciencescripts/'
db1 = Server_Production
db2 = Server_SCV
db3 = Server_Search
#########################################################

with open(base_dir+'Cred.yaml', 'r') as f:
    doc = yaml.load(f)

hostname = doc[db1]['host']
username = doc[db1]['user']
password = doc[db1]['password']
database = doc[db1]['db']
cursorclass = pymysql.cursors.DictCursor


host = 'sql.office.ohsuper.com'
con = pymysql.connect(host = hostname, user = username, password = password, db = database)


q1 = """
    select cp.id as cart_promotion_id, cp.title as promotion_name, from_unixtime(cp.starts_at) as live_date, fbp.product_id, p.name as product_name  from cart_promotions as cp
    inner join filter_buckets__products as fbp on fbp.filter_bucket_id = cp.item_collector_ids
    inner join products as p on p.id = fbp.product_id
    ;
    """

bucket = pd.read_sql(q1, con)

bucket = bucket[bucket["live_date"]>= datetime.now()]
cut_off_date = datetime.now() + timedelta(days=2)
bucket = bucket[bucket["live_date"]< cut_off_date]


q3 = """
    select u.email, wi.id as wishlist_item_id, wi.sku_id, s.sku_code, p.id as product_id, stv.value as size ,wi.user_id, wi.sku_id, from_unixtime(wi.created_at) as wi_date from wishlist_items as wi
    inner join skus as s on s.id = wi.sku_id
    inner join users as u on u.id = wi.user_id
    inner join products as p on p.id = s.product_id
    inner join size_template_values as stv on s.size_template_value_id = stv.id
    ;
    """


wishlist = pd.read_sql(q3, con)



con2 = pymysql.connect(host, "qlikview", "F3WxH7AkJP85", "reservations")

q3 = """
    select d.sku_code, d.distribution_centre_id, d.physical_stock from reservation_dcsku as d
    ;
    """
stock = pd.read_sql(q3, con2)
stock_jhb = stock[stock["distribution_centre_id"]==9]
stock_cpt = stock[stock["distribution_centre_id"]==8]

stock_cpt = stock_cpt.rename(columns={"physical_stock":"cpt_stock"})
stock_jhb = stock_jhb.rename(columns={"physical_stock":"jhb_stock"})

stock2 = pd.merge(stock_cpt, stock_jhb, how="inner", on="sku_code")
stock2 = stock2.drop(["distribution_centre_id_x", "distribution_centre_id_y"], axis=1)
stock2["stock"] = stock2["cpt_stock"]+  stock2["jhb_stock"]

stock2 = stock2.drop(["cpt_stock", "jhb_stock"], 1)
stock = stock2

stock_wish = pd.merge(wishlist, stock, how="inner", on="sku_code")
stock_wish = stock_wish[stock_wish["stock"]>0]

combined = pd.merge(stock_wish, bucket, on="product_id", how="inner")
combined = combined.drop(["wishlist_item_id", "sku_code", "product_id"], 1)

combined.to_csv("wishlist_example.csv")



smtpUser = 'ds1superbalist@gmail.com' #email of sender
smtpPass = 'datascience' #password of sender
#'darnell.j@superbalist.com', 'amy.m@superbalist.com',
toAdd = [ 'gary.w@superbalist.com'] #recipient
fromAdd = smtpUser

today = date.today()

subject  = 'Wishlist email %s' % today.strftime('%Y %b %d')
header =  'From : ' + fromAdd + '\n' + 'Subject : ' + subject + '\n'
body = 'This is an automated mail containing the wish list products going on offer tomorrow  %s' % today.strftime('%Y %b %d')

attach = "wishlist_example.csv"  #file to attach

print (header)


def sendMail(to, subject, text, files=[]):
    assert type(to)==list
    assert type(files)==list

    msg = MIMEMultipart()
    msg['From'] = smtpUser
    msg['To'] = COMMASPACE.join(to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach( MIMEText(text) )

    for file in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(file,"rb").read() )
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"'
                       % os.path.basename(file))
        msg.attach(part)

    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo_or_helo_if_needed()
    server.starttls()
    server.ehlo_or_helo_if_needed()
    server.login(smtpUser,smtpPass)
    server.sendmail(smtpUser, to, msg.as_string())

    print ('Done')

    server.quit()

print(4)
sendMail( toAdd, subject, body, [attach] )
print(5)
