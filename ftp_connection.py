#from apps.marketplaces.services.ftp_connection import ftp_connection_rakuten
from apps.advertisers.models import Advertiser, FeedFileLog
from apps.products.models import BrandRakuten
from apps.marketplaces.services.create_product_instance_task import create_product_instance
from apps.marketplaces.models import MarketPlace
from django.core.mail import send_mail


import os
import gzip
import xmltodict

from django.utils import timezone
from pymongo import MongoClient
from ftplib import FTP
from lxml import etree

MONGO_HOST = os.environ.get('MONGO_HOST', '0 0.0.0.0:27017')
MONGO_USER = os.environ.get('MONGO_USER')
MONGO_PASS = os.environ.get('MONGO_PASS')


def parse_file(xml, docs, advertiser):
    """
    Parses xml file to dict type and save instance in to Mongo doc
    """

    context = etree.iterparse(xml, events=("start", "end"))
    is_first = True
    for event, node in context:
        if is_first:
            root = node
            is_first = False
        if event == "end" and node.tag == "product":
            if node.text:
                url = node.text
                continue
            doc = etree.tostring(node, pretty_print=True)
            doc = xmltodict.parse(doc)
            doc['product']['URL']['product'] = url
            data = dict(product=doc['product'], advertiser=advertiser)
            docs.insert_one(data)
            node.clear()
            while node.getprevious() is not None:
                del node.getparent()[0]
            root.clear()


def download_file(feed_file, marketplace, ftp):

    """
    Download feed file from Rakuten platform and storage in locally for parsing
    """
    print(f'{feed_file} started download')
    feed_file.status = 'in progress'
    feed_file.save()
    sid = marketplace.sid
    advertiser = feed_file.advertiser
    mid = advertiser.mid
    criteria = ''
    file_name = f'{mid}_{sid}_mp{criteria}.xml.gz'
    local_file = f'storage/{mid}_{sid}_mp{criteria}.xml.gz'
    try:
        with open(local_file, 'wb') as f:
            ftp.retrbinary('RETR ' + file_name, f.write)
    except Exception as e:
        feed_file.status = 'error'
        feed_file.save()
        print(e)
        return
    print(f'{feed_file} finished download')


def parse_advertiser(feed_file, marketplace):

    """
    Opens local  file and prepares it to parsing
    """
    print(f'{feed_file} started parsing')
    sid = marketplace.sid
    advertiser = feed_file.advertiser
    mid = advertiser.mid
    criteria = ''
    local_file = f'storage/{mid}_{sid}_mp{criteria}.xml.gz'
    client = MongoClient(MONGO_HOST, username=MONGO_USER, password=MONGO_PASS)
    db = client.docdb
    docs = db.docs
    advertiser = feed_file.advertiser.id
    parse_file(gzip.open(local_file), docs, advertiser)
    print(f'{feed_file} finished parsing')
    client.close()
    #os.remove(local_file)
    feed_file.status = 'done'
    feed_file.save()
    print(f'{feed_file} finished')


def ftp_connection_rakuten():
    """
    Creates FTP connection to Rakuten platform, downloads and parses files and creates products
    """

    marketplace = 'Rakuten'
    try:
        marketplace = MarketPlace.objects.get(name=marketplace)
    except Exception as e:
        print(e)
    user = marketplace.ftp_username
    host = marketplace.ftp_host
    env_var = marketplace.name.upper() + '_PASSWORD'
    passwd = os.environ.get(env_var, None)
    if not passwd:
        raise ValueError('password not found')

    advertises = Advertiser.objects.filter(marketplace_id=marketplace) # will be used all if needed
    for advertiser in advertises[:3]:
        FeedFileLog.objects.create(advertiser=advertiser)
    client = MongoClient(MONGO_HOST, username=MONGO_USER, password=MONGO_PASS)
    db = client.docdb
    db.docs.drop()
    client.close()
    feed_files = FeedFileLog.objects.filter(date_created__date=timezone.now().date(), status='unprocessed')

    ftp = FTP('')
    ftp.connect(host)
    ftp.login(user=user, passwd=passwd)
    for feed_file in feed_files:
        download_file(feed_file, marketplace, ftp)
    try:
        ftp.quit()
    except Exception:
        ftp.close()
    counter = 0
    while True:
        feed_files = FeedFileLog.objects.filter(date_created__date=timezone.now().date(), status='error')
        print(f'{counter} iteration of error handling')
        if not feed_files or counter == 5:
            break
        try:
            ftp = FTP('')
            ftp.connect(host)
            ftp.login(user=user, passwd=passwd)
        except Exception as e:
            print(e)
            continue
        for feed_file in feed_files:
            download_file(feed_file, marketplace, ftp)
        try:
            ftp.quit()
        except Exception:
            ftp.close()
        counter += 1

    feed_files = FeedFileLog.objects.filter(date_created__date=timezone.now().date(), status='in progress')
    print('before parsing')
    for feed_file in feed_files:
        parse_advertiser(feed_file, marketplace)

    print('before saving')
    client = MongoClient(MONGO_HOST, username=MONGO_USER, password=MONGO_PASS)
    db = client.docdb
    docs = db.docs
    brands = BrandRakuten.objects.all()
    list_names = [''.join([a for a in brand.name.lower() if a.isalpha() or a.isdigit()]) for brand in brands]
    list_pks = [brand.pk for brand in brands]
    result = []
    result.append(list_pks)
    result.append(list_names)
    while True:
        item = docs.find_one()
        if not item:
            break
        product = item.get('product')
        advertiser = item.get('advertiser')
        if not product or not advertiser:
            continue
        try:
            create_product_instance(product, result)
        except Exception as e:
            pass
        docs.delete_one(item)

    print('complete')
    feed_files = FeedFileLog.objects.filter(date_created__date=timezone.now().date(), status='error')
    if feed_files:
        names = [feed.advertiser.name for feed in feed_files]
        send_mail('Errors from Revelle',
                  f'Updating errors happen with {names}',
                  'revelle@support.com',
                  ['akhtyrtsev@gmail.com', 'olga@wstlnk.com'],
                    fail_silently=False
                    )

