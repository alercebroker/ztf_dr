import pandas as pd

CHECKSUMS_TEST = pd.DataFrame({
    "field": ["0202", "0202", "0202"],
    "checksum": ["8719c8505aa2a7f47fc9b195e64aba02",
                 "88e06637b21e7aab94f178dcb911585a",
                 "f597201471d7e188f1786c606c9614cf"],
    "file": ["https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5//0/field0202/ztf_000202_zg_c10_q1_dr5.parquet",
             "https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5//0/field0202/ztf_000202_zg_c14_q3_dr5.parquet",
             "https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5//0/field0202/ztf_000202_zg_c16_q2_dr5.parquet"]
})

EXPECTED_REQUEST_RESPONSE = '''<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /data/ZTF/lc_dr5/0/field0202</title>
 </head>
 <body>
<h1>Index of /data/ZTF/lc_dr5/0/field0202</h1>
<pre><img src="/icons/blank.gif" alt="Icon "> <a href="?C=N;O=D">Name</a>                             <a href="?C=M;O=A">Last modified</a>      <a href="?C=S;O=A">Size</a>  <a href="?C=D;O=A">Description</a><hr><img src="/icons/unknown.gif" alt="[   ]"> <a href="ztf_000202_zg_c10_q1_dr5.parquet">ztf_000202_zg_c10_q1_dr5.parquet</a> 2021-03-12 15:05  313K  
<img src="/icons/unknown.gif" alt="[   ]"> <a href="ztf_000202_zg_c14_q3_dr5.parquet">ztf_000202_zg_c14_q3_dr5.parquet</a> 2021-03-12 15:05  476K  
<img src="/icons/unknown.gif" alt="[   ]"> <a href="ztf_000202_zg_c16_q2_dr5.parquet">ztf_000202_zg_c16_q2_dr5.parquet</a> 2021-03-12 15:05  468K  
<hr></pre>
</body></html>
'''


DF_EXAMPLE = """file,date,size
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0202/ztf_000202_zg_c10_q1_dr5.parquet,2021-03-12 15:05,313K  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0202/ztf_000202_zg_c14_q3_dr5.parquet,2021-03-12 15:05,476K  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0202/ztf_000202_zg_c16_q2_dr5.parquet,2021-03-12 15:05,468K  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c01_q1_dr5.parquet,2021-03-11 17:01,2.1M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c01_q2_dr5.parquet,2021-03-11 17:01,2.1M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c01_q3_dr5.parquet,2021-03-11 17:01,2.1M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c01_q4_dr5.parquet,2021-03-11 17:01,1.9M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c02_q1_dr5.parquet,2021-03-11 17:01,2.0M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c02_q2_dr5.parquet,2021-03-11 17:01,2.1M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c02_q3_dr5.parquet,2021-03-11 17:01,2.2M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c02_q4_dr5.parquet,2021-03-11 17:01,2.4M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c03_q1_dr5.parquet,2021-03-11 17:01,2.3M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c03_q2_dr5.parquet,2021-03-11 17:01,2.2M"""