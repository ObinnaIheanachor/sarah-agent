import sqlite3, shutil

db_path = "cache/insolvency_cache.db"
shutil.copyfile(db_path, db_path + ".backup")

con = sqlite3.connect(db_path)
cur = con.cursor()

cur.execute("BEGIN")
cur.execute("""
UPDATE document_cache
SET document_url = 'chdoc://' || substr(
  document_url,
  instr(document_url, '/document/') + length('/document/')
)
WHERE document_url LIKE '%/document/%'
""")
con.commit()

# optional sanity checks
n = cur.execute("SELECT COUNT(*) FROM document_cache WHERE document_url LIKE 'chdoc://%'").fetchone()[0]
print("Rows with chdoc:// now:", n)

cur.execute("VACUUM")  # optional
con.close()
print("Done.")
