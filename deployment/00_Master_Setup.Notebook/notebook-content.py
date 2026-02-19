# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

# 1. INSTALACIÓN AUTOMÁTICA (La línea mágica que faltaba)
# Esto asegura que la librería exista antes de intentar importarla
%pip install semantic-link-labs

# 2. IMPORTACIÓN Y LÓGICA
import sempy_labs as sl
import time

# --- CONFIGURACIÓN AUTOMÁTICA ---
# Al dejarlo en None, se aplicará al workspace y lakehouse actuales del notebook
MODEL_NAME = "F1_Gold_Model"  # Asegúrate de que este sea el nombre exacto

print(f"🔄 Iniciando remapeo automático del modelo: {MODEL_NAME}")

try:
    # 1. Remapear la conexión (Cablear al Lakehouse actual)
    print("   🔌 Buscando Lakehouse adjunto para reconectar...")
    sl.directlake.update_direct_lake_model_connection(
        dataset = MODEL_NAME,
        source_type = "Lakehouse",
        use_sql_endpoint = True
    )
    print("   ✅ Conexión remapeada con éxito.")

    # 2. Sincronizar el esquema (Leer las tablas nuevas)
    print("   🔄 Sincronizando esquema...")
    sl.directlake.direct_lake_schema_sync(
        dataset = MODEL_NAME
    )
    print("   ✅ Esquema sincronizado.")

    print("\n🚀 ¡LISTO! El modelo ya apunta a tus datos locales.")

except Exception as e:
    print(f"\n❌ Error: {e}")
    print("💡 PISTA: ¿Has añadido un Lakehouse al panel izquierdo ('Lakehouses') de este notebook?")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
