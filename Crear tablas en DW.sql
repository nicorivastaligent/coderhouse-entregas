CREATE TABLE ripto_price_history(
    id 				 VARCHAR(255) not null,
    name 		 	 VARCHAR(255) not null,
    priceusd 		 float8 not null ,
    date_time 		 TIMESTAMP not null ,
    key_id_date_time VARCHAR(255) primary key 
);

CREATE TABLE cripto_price_new_data(
    id 				 VARCHAR(255) not null,
    name 		 	 VARCHAR(255) not null,
    priceusd 		 float8 not null ,
    date_time 		 TIMESTAMP not null ,
    key_id_date_time VARCHAR(255) primary key 
);