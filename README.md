Sebelum Mengekseksui file diatas, perlu diperhatikan bahwa Django hanya akan berjalan dalam virtual environtment.

Cara aktivasi virtual environtment adalah dengan aksess {HOME_DIR}\djagoenv\Scripts\activate

File neo4js tersedia dalam link berikut : https://drive.google.com/drive/folders/1l1EIayjVyV_CY-6nTIHysSt1h1Wk_n_O?usp=sharing
Jangan lupa install Java Server yg telah tersedia di dalamnya.

Adapun cara menyalakan Neo4js adalah dengan akses {HOME_DIR}\neo4js\neo4j-community-3.5.6\bin\neo4j console

Default Username dan Password pada Neo4j adalah
user : neo4j
pass : neo4j

sehingga diperlukan mengganti settings django dibagian process_mining\settings.py bagian NEO4J_USER dan NEO4J_PASS

File Pip requirements ada di process_mining\requirements.txt
