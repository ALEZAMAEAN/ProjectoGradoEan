-- Create table for Dim_Tiempo
CREATE TABLE Dim_Tiempo (
    Id_fecha DATE PRIMARY KEY,
    Año INT,
    Mes INT,
    Dia INT
);

-- Create table for Dim_Tipo_Delito
CREATE TABLE Dim_Tipo_Delito (
    Id_Tipo_Delito INT PRIMARY KEY,
    Nom_Tipo_Delito VARCHAR(150),
    Desc_Tipo_Delito VARCHAR(150)
);

-- Create table for Dim_Ubicacion

CREATE TABLE Dim_Ubicacion (
    Id_ubicacion BIGINT PRIMARY KEY,
    Tipo_ubicacion VARCHAR(50),
    Desc_ubicacion VARCHAR(50),
    Longitud VARCHAR(100),
    Latitud VARCHAR(100)
);

-- Create table for Dim_Arma
CREATE TABLE Dim_Arma (
    Id_Arma INT PRIMARY KEY,
    Nom_Arma VARCHAR(50)
);

-- Create table for Fact_Delitos
CREATE TABLE Fact_Delitos (
    Id_delitos BIGINT IDENTITY(1,1) PRIMARY KEY,
    Id_fecha DATE,
    Id_ubicacion BIGINT,
    Id_tipo_delito INT,
    Id_arma INT,
    Num_cantidad INT,
    FOREIGN KEY (Id_fecha) REFERENCES Dim_Tiempo(Id_fecha),
    FOREIGN KEY (Id_ubicacion) REFERENCES Dim_Ubicacion(Id_ubicacion),
    FOREIGN KEY (Id_tipo_delito) REFERENCES Dim_Tipo_Delito(Id_Tipo_Delito),
    FOREIGN KEY (Id_arma) REFERENCES Dim_Arma(Id_Arma)
);
