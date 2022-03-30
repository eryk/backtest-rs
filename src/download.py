import glob
import hashlib
import multiprocessing
import os
import time
import traceback
import xml.dom.minidom
import zipfile

import requests

download_base_dir = "/Users/eryk/data/rawdata/"
unzip_base_dir = "/Users/eryk/data/market_data/"
base_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix="
zip_base_url = "https://data.binance.vision/"
market_list = ["spot", "futures/cm", "futures/um"]
data_type_list = ["klines"]
period_list = ["5m", "15m", "1m", "30m", "1h", "6h", "1d"]


def exists(path):
    return os.path.exists(path)


def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def file_stat(path, time_fmt=False):
    """
    ctime,atime,mtime,size
    :param path:
    :param time_fmt:
    :return:
    """

    def get_size():
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return total_size

    if os.path.exists(path):
        ctime = os.stat(path).st_ctime  # change time
        atime = os.stat(path).st_atime  # access time
        mtime = os.stat(path).st_mtime  # modify time
        if os.path.isdir(path):
            size = get_size()
        else:
            size = os.stat(path).st_size
        if time_fmt:
            ctime = int(ctime * 1e6)
            atime = int(atime * 1e6)
            mtime = int(mtime * 1e6)
        return {'mtime': mtime, 'atime': atime, 'ctime': ctime, 'size': size}

def decompress_zip(file_name, unzip_path):
    """unzip zip file"""
    zip_file = zipfile.ZipFile(file_name)
    if os.path.isdir(unzip_path):
        pass
    else:
        mkdir(unzip_path)
    for names in zip_file.namelist():
        zip_file.extract(names, unzip_path)
    zip_file.close()

def download_zip(zip_url_suffix, target_dir):
    try:
        mkdir(target_dir)
        zip_url = "%s%s" % (zip_base_url, zip_url_suffix)
        file_name = zip_url.split("/")[-1]
        target_filepath = target_dir + "/" + file_name
        if exists(target_filepath) and file_stat(target_filepath)['size'] > 0:
            return True, ""
        zip_response = requests.get(zip_url)
        with open(target_filepath, "wb") as f:
            f.write(zip_response.content)
        checksum_url = zip_url + '.CHECKSUM'
        checksum_response = requests.get(checksum_url)
        with open(target_filepath + '.CHECKSUM', "wb") as f:
            f.write(checksum_response.content)
        print(zip_url, target_filepath)
    except Exception as e:
        print(traceback.format_exc())
        return False, zip_url_suffix
    return True, ""


def checksum(checksum_path):
    with open(checksum_path, "r") as f:
        fields = f.readline().split(" ")
        (shasum, filename) = list(filter(lambda field: field != "", fields))[:2]
        sha256_hash = hashlib.sha256()
        with open(checksum_path[:-9], "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
            if shasum != sha256_hash.hexdigest():
                print("checksum error: %s" % checksum_path[:-9])
                return False
    return True


def check_zip_dir():
    for market in market_list:
        for fp in glob.glob("%s%s/*/*.CHECKSUM" % (download_base_dir, market)):
            checksum(fp)


def unzip_dir():
    for market in market_list[:1]:
        for fp in glob.glob("%s%s/*/*.zip" % (download_base_dir, market))[:1]:
            fields = fp.split("/")[-1].split("-")
            market = fp.split("/")[-3]
            (pair, data_type, bizdate) = fields[0], fields[1], "".join(fields[2:])[:-4]
            unzip_dir = "%s%s/%s/%s" % (unzip_base_dir, market, pair, data_type)
            mkdir(unzip_dir)
            unzip_fp = unzip_dir + "/" + fp.split("/")[-1][:-4] + '.csv'
            if not exists(unzip_fp):
                print("unzip:", unzip_fp)
                decompress_zip(fp, unzip_dir)


def fetch_pair_list_url(market, data_type, period):
    fetch_url = "%sdata/%s/daily/%s/" % (base_url, market, data_type)
    print("process:", fetch_url)
    response = requests.get(fetch_url)
    if response.status_code != 200:
        print("err:", fetch_url)
    dom = xml.dom.minidom.parseString(response.content)
    nodes = dom.documentElement.getElementsByTagName("Prefix")
    pairs = []

    pool = multiprocessing.Pool(processes=8)
    for url in nodes[1:]:
        url_str = url.firstChild.nodeValue
        pairs.append(url_str.split('/')[-2])
    for pair in pairs[:]:
        if not (pair.endswith('BTC')) or pair.endswith("USDT"):
            continue
        download_counter = {"total": 0, "success": 0, "error": 0}
        failed_zip_url = []
        result = []
        data_type_detail_url = "%s%s/%s/" % (fetch_url, pair, period)
        zip_list = fetch_pair_daily_list(data_type_detail_url)
        if zip_list is None:
            continue
        for url in zip_list:
            bizdate = "".join(url.split('-')[-3:])[:-4]
            result.append(pool.apply_async(download_zip, (url, "%s%s/%s" % (download_base_dir, market, bizdate))))
            download_counter['total'] += 1
        for res in result:
            is_success, zip_url_suffix = res.get()
            if is_success:
                download_counter['success'] += 1
            else:
                download_counter['error'] += 1
                failed_zip_url.append(zip_url_suffix)
        print(download_counter)
        print(failed_zip_url)
    pool.close()
    pool.join()


def fetch_pair_daily_list(url):
    print("fetch_pair_daily_list:", url)
    retry = 0
    while retry <= 3:
        try:
            response = requests.get(url)
            if response.status_code != 200:
                print("err:", url)
            dom = xml.dom.minidom.parseString(response.content)
            nodes = dom.documentElement.getElementsByTagName("Key")
            results = []
            for node in nodes:
                url = node.firstChild.nodeValue
                if url.endswith(".zip"):
                    results.append(url)
            return results
        except Exception as e:
            print(traceback.format_exc())
            retry += 1
            time.sleep(retry)
    print("failed to fetch_pair_daily_list:", url)



def download_history():
    for data_type in data_type_list[:1]:
        for market in market_list[:]:
            for period in period_list[:2]:
                print(market, data_type, period)
                fetch_pair_list_url(market, data_type, period)


def daily_update():
    pass


if __name__ == '__main__':
    download_history()
    check_zip_dir()
    unzip_dir()
