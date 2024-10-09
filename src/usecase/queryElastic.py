from pandas import json_normalize  # Mengimpor fungsi json_normalize untuk meratakan data JSON
from elasticsearch import Elasticsearch, helpers  # Mengimpor kelas Elasticsearch dan helpers dari modul elasticsearch

# Koneksi ke instance Elasticsearch yang berjalan di Docker
es = Elasticsearch(
    ["http://localhost:9200"],  # URL untuk mengakses Elasticsearch
    request_timeout=30,  # Mengatur waktu tunggu permintaan menjadi 30 detik
    max_retries=10,  # Mengatur jumlah maksimum upaya pengulangan permintaan yang gagal menjadi 10
    retry_on_timeout=True  # Mengaktifkan pengulangan jika permintaan melebihi batas waktu
)

# Melakukan pencarian awal dengan scroll
res = es.search(
    index='users',  # Menentukan indeks yang ingin dicari
    scroll='20m',  # Mengatur waktu untuk scroll menjadi 20 menit
    size=500,  # Mengambil 500 dokumen per permintaan
    body={  # Body dari query pencarian
        "query": {
            "match_all": {}  # Mengambil semua dokumen dalam indeks
        }
    }
)

sid = res['_scroll_id']  # Mendapatkan scroll ID untuk digunakan dalam permintaan selanjutnya
size = res['hits']['total']['value']  # Mendapatkan total jumlah dokumen yang ditemukan

# Melakukan scrolling untuk mengambil semua dokumen
while (size > 0):  # Selama masih ada dokumen yang tersisa
    res = es.scroll(scroll_id=sid, scroll='20m')  # Melakukan permintaan scroll dengan scroll ID yang didapat
    sid = res['_scroll_id']  # Memperbarui scroll ID
    size = len(res['hits']['hits'])  # Menghitung jumlah dokumen yang ditemukan pada permintaan ini

    # Iterasi dan mencetak setiap dokumen
    for doc in res['hits']['hits']:  # Menggunakan loop untuk mengiterasi setiap dokumen yang ditemukan
        print(doc['_source'])  # Mencetak bagian _source dari setiap dokumen, yang berisi data asli
