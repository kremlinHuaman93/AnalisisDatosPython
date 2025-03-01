# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS retail_bronze CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS retail_bronze;
# MAGIC USE retail_bronze;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Definir la base de datos donde están las tablas
database_name = "retail_bronze"

# Crear la base de datos si no existe
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# Lista de tablas y archivos CSV con sus respectivos esquemas
tables = {
    "departments": ("/Volumes/workspace/default/data/departments", StructType([
    StructField('department_id',   IntegerType(), nullable=True),
    StructField('department_name', StringType(), nullable=True)
    ])),
    "categories": ("/Volumes/workspace/default/data/categories",StructType([
    StructField('category_id',            IntegerType(), nullable=True),
    StructField('category_department_id', IntegerType(), nullable=True),
    StructField('category_name',          StringType(), nullable=True)
    ])),
    "customers": ("/Volumes/workspace/default/data/customers", StructType([
    StructField('customer_id',       IntegerType(), nullable=True),
    StructField('customer_fname',    StringType(), nullable=True),
    StructField('customer_lname',    StringType(), nullable=True),
    StructField('customer_email',    StringType(), nullable=True),
    StructField('customer_password', StringType(), nullable=True),
    StructField('customer_street',   StringType(), nullable=True),
    StructField('customer_city',     StringType(), nullable=True),
    StructField('customer_state',    StringType(), nullable=True),
    StructField('customer_zipcode',  StringType(), nullable=True)])),
    "orders": ("/Volumes/workspace/default/data/orders", StructType([
    StructField('order_id',          IntegerType(), nullable=True),
    StructField('order_date',        StringType(), nullable=True),
    StructField('order_customer_id', IntegerType(), nullable=True),
    StructField('order_status',      StringType(), nullable=True)
    ])),
    "products": ("/Volumes/workspace/default/data/products", StructType([
    StructField('product_id',          IntegerType(), nullable=True),
    StructField('product_category_id', IntegerType(), nullable=True),
    StructField('product_name',        StringType(), nullable=True),
    StructField('product_description', StringType(), nullable=True),
    StructField('product_price',       FloatType(), nullable=True),
    StructField('product_image',       StringType(), nullable=True)
    ])),
    "order_items": ("/Volumes/workspace/default/data/order_items", StructType([
    StructField('order_item_id',            IntegerType(), nullable=True),
     StructField('order_item_order_id',      IntegerType(), nullable=True),
    StructField('order_item_product_id',    IntegerType(), nullable=True),
    StructField('order_item_quantity',      IntegerType(), nullable=True),
     StructField('order_item_subtotal',      FloatType(), nullable=True),
     StructField('order_item_product_price', FloatType(), nullable=True)
     ]))
}

# Iterar sobre cada tabla y cargar los datos en la tabla Delta
for table_name, (file_path, schema) in tables.items():
    print(f"Procesando {table_name}...")

    # Leer el archivo CSV con el esquema correcto
    df = spark.read.csv(file_path, schema=schema, sep="|", header=False)

    # Escribir los datos en formato Delta dentro de la base de datos
    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")

    print(f"Datos insertados en la tabla {database_name}.{table_name}")

print("Ingesta de datos en las tablas finalizada.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail_bronze.departments

# COMMAND ----------

