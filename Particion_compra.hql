CREATE DATABASE IF NOT EXISTS integrador3;
USE integrador3;

DROP TABLE IF EXISTS compra;
CREATE EXTERNAL TABLE IF NOT EXISTS compra (
  IdCompra				INTEGER,
  IdProducto			INTEGER,
  Cantidad			  INTEGER,
  Precio				  FLOAT,
  IdProveedor			INTEGER
)
PARTITIONED BY (Fecha INT)
STORED AS PARQUET
LOCATION '/data3/compra'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


INSERT INTO compra
PARTITION(Fecha = 2015)
SELECT
	IdCompra,
	IdProducto,
	Cantidad,
	Precio,
	IdProveedor
FROM 
  integrador.compra
WHERE 
  YEAR(Fecha) = 2015;

INSERT INTO compra
PARTITION(Fecha = 2016)
SELECT
	IdCompra,
	IdProducto,
	Cantidad,
	Precio,
	IdProveedor
FROM 
  integrador.compra
WHERE 
  YEAR(Fecha) = 2016;

INSERT INTO compra
PARTITION(Fecha = 2017)
SELECT
	IdCompra,
	IdProducto,
	Cantidad,
	Precio,
	IdProveedor
FROM 
  integrador.compra
WHERE 
  YEAR(Fecha) = 2017;

INSERT INTO compra
PARTITION(Fecha = 2018)
SELECT
	IdCompra,
	IdProducto,
	Cantidad,
	Precio,
	IdProveedor
FROM 
  integrador.compra
WHERE 
  YEAR(Fecha) = 2018;

INSERT INTO compra
PARTITION(Fecha = 2019)
SELECT
	IdCompra,
	IdProducto,
	Cantidad,
	Precio,
	IdProveedor
FROM 
  integrador.compra
WHERE 
  YEAR(Fecha) = 2019;

INSERT INTO compra
PARTITION(Fecha = 2020)
SELECT
	IdCompra,
	IdProducto,
	Cantidad,
	Precio,
	IdProveedor
FROM 
  integrador.compra
WHERE 
  YEAR(Fecha) = 2020;