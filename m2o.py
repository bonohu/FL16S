# -*- coding: utf-8 -*-

import sqlite3
import argparse
from urllib.request import Request, urlopen
import csv
import re
import xml.etree.ElementTree as ET
from typing import List, Dict
import collections
import json

# BLASTの結果のtextファイル名をパラメータから取得
# $ python m2o.py <file path> のようにBLASTのformat 6 outputファイルを引数にしてスクリプトを呼ぶ
parser = argparse.ArgumentParser()
parser.add_argument('arg1', type=argparse.FileType('r'))
args = parser.parse_args()

ncbi_nucleotide_base = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&id={}&rettype=fasta&retmode=xml'

target_db = "data/orgname_2021_4"
tbl = "sid_tax_orgname"
file_out = "data/population.json"


def main():
    """
    BLASTの結果（format6 textファイル）を入力として
    outputに含まれるsequence idをorganism nameに変換しつつ種レベルでカウントする
    :return: [[taxonomy, organism name, count, ratio],,]
    """
    # dbを初めて作る場合は実行。使いまわしたい場合は一旦dropすることになるので、コメントアウトしておく
    # create_table()

    # textを読み込み一行ごとsequence idを取得
    sids = get_sequence_id()
    # sequence id を使ってNCBI Nucleotideを検索し、taxonomyとorganism nameを取得し保存
    # データベースが保存済みであればコメントアウトしても問題ない
    for i in sids:
        # dbにsequence idが登録されていなければデータを取得する
        if is_new_id(i):
            store_orgname(get_genbank_data(i))

    # BLASTの結果（各リードの類似度一位の配列）からseqquence idの数をカウントし、
    # さらにorganismかtaxonomyにマッピングし再集計する
    count_tax = count_orgname(sids)
    # リード全体の中のtaxonomyの割合を追加
    count_tax_p = add_percentage_value(len(sids), count_tax)
    return count_tax_p


def get_sequence_id():
    """
    BLASTの各出力（format 6の各行）からsequence id を取得する
    :return: sequence_id
    """
    f = args.arg1
    ids = []
    with open(f.name, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            # id+ _N | 属名が文字列としてrowに含まれるので_, |で分割
            s = re.split('[_|]', row[1])[0]
            ids.append(s)

    return ids
    # c = collections.Counter(ids)
    # [(id, num),,]を降順に書き出す
    # top_n = c.most_common(20)


def create_table():
    """
    sequence id-orgname,taxonomyを保存するテーブルを作る
    :return:
    """
    sql_orgname = """
        create table {}(
            sid text PRIMARY KEY,
            orgname text,
            taxonomy text
        );
    """
    conn = sqlite3.connect(target_db)
    cur = conn.cursor()
    cur.execute('SELECT count(*) FROM {}'.format(tbl))
    is_exist = len(cur.fetchone()[0])
    if not is_exist:
        cur.execute(sql_orgname.format(tbl))
    conn.commit()
    conn.close()


def store_orgname(n:tuple):
    """
    sequence idをキーにorgname（とtaxonomy id）を保存する
    保存先はとりあえずsqlite
    :param t:
    :return:
    """
    conn = sqlite3.connect(target_db)
    cur = conn.cursor()
    # 一致するsequence id（sid）が登録されていなければ新たにdbにデータを保存する
    cur.execute('INSERT INTO {} VALUES {}'.format(tbl, n))
    conn.commit()
    conn.close()


def is_new_id(i):
    """"
    sequence idが登録済みか否か返す
    """
    conn = sqlite3.connect(target_db)
    cur = conn.cursor()
    q = 'SELECT * from {} where sid="{}"'.format(tbl, i)
    cur.execute(q)
    is_new = True if len(cur.fetchall()) == 0 else False
    return is_new


def get_genbank_data(sid) -> tuple:
    """
    sequence idをクエリにEFetchでGenbankデータを取得し、orgname, taxonomyを
    :return: (sid, taxonomy:str, organism name:str )
    """
    url = ncbi_nucleotide_base.format(sid)
    req = Request(url)
    with urlopen(req) as res:
        xml_data = res.read()
        root = ET.fromstring(xml_data)
        for v in root.iter('TSeq_taxid'):
            taxonomy = v.text
        for v in root.iter('TSeq_orgname'):
            orgname = v.text

    return sid, taxonomy, orgname


def count_orgname(sids) -> List[dict]:
    """
    sequence idのlistをtaxonomyのlistにマッピングし、taxonomyをカウントする
    :return:
    """
    sid_taxonomy = map(lambda x: get_taxonomy(x), sids)
    # c = collections.Counter(sid_taxonomy)
    # cはタプルのCounterオブジェクトだが、taxonomyでcountすると
    t = [x[1] for x in sid_taxonomy]
    c = collections.Counter(t)
    tax_count = c.most_common()
    tax_count = [get_orgname(x) for x in tax_count]
    return tax_count


def get_taxonomy(sid: str) -> tuple:
    """
    sequence idを引数に保存したデータから(sid, taxonomy, organism name)のタプルを生成し返す
    :param sid:
    :return:
    """
    conn = sqlite3.connect(target_db)
    cur = conn.cursor()
    q = 'SELECT * from {} where sid="{}"'.format(tbl, sid)
    cur.execute(q)
    sid_tax_org = cur.fetchone()
    conn.close()
    return sid_tax_org


def get_orgname(tax_c) -> tuple:
    """
    taxonomyを引数にorganism nameを追加
    :param tax:
    :return:
    """
    conn = sqlite3.connect(target_db)
    cur = conn.cursor()
    t = tax_c[0]
    q = 'SELECT orgname from {} where taxonomy="{}"'.format(tbl, t)
    cur.execute(q)
    n = cur.fetchone()
    tax_c = list(tax_c)
    tax_c.insert(1, n[0])
    conn.close()
    return tax_c


def add_percentage_value(l:int, to:List[list]) -> List[list]:
    """
    counter.most_commonしたtaxonomyリストに割合を追加する
    :param sids:
    :param sto:
    :return:
    """
    for d in to:
        d.append(d[2]/l)

    return to


def test_count_taxonomy():
    sids = get_sequence_id()
    res = count_orgname(sids)
    res = add_percentage_value(len(sids), res)
    print(res)


def test_get_taxonomy(sid):
    get_taxonomy(sid)


if __name__ == "__main__":
    population = main()
    with open(file_out, 'w') as f:
        json.dump(population, f)
