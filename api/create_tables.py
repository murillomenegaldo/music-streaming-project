from data.db import engine, Base
from api.models import Song

# Cria as tabelas no banco (caso ainda não existam)
print("🔄 Criando tabelas no banco de dados...")
Base.metadata.create_all(bind=engine)
print("✅ Tabelas criadas com sucesso!")
