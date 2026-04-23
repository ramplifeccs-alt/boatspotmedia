
from app.db.connection import get_db

def update_storage(creator_id, file_size):
    db = get_db()
    cur = db.cursor()

    cur.execute(
        "UPDATE creators SET storage_used_gb = storage_used_gb + %s WHERE id=%s",
        (file_size / (1024**3), creator_id)
    )

    db.commit()
    cur.close()
