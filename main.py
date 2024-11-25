from fastapi import FastAPI, UploadFile, HTTPException, File
from fastapi import Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import  List, Dict, Optional
from lxml import etree
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, cpu_count
from datetime import datetime
from bson import ObjectId
import asyncio
import os
import tempfile
import xml.dom.minidom
import xmltodict
import json
import motor.motor_asyncio
import tarfile
import time
import magic
import io
import uvicorn

app = FastAPI()

MONGO_DETAILS = "mongodb://localhost:27017/"
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
database = client.xml_database
collection1 = database.get_collection("archivos_individual")
collection2 = database.get_collection("archivos_pasoapaso")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/formatearXML/")
async def format_xml(file: UploadFile, file_name: str = Form(...)):
    if file.content_type not in ["text/xml", "application/xml", "application/octet-stream"]:
        raise JSONResponse(content={"status": "invalid", "message": "Invalid file type"} ,status_code=400)
    
    try:
        contents = await file.read()

        try:
            dom = xml.dom.minidom.parseString(contents)
            pretty_xml = dom.toprettyxml(indent="    ")
            return JSONResponse(content={"status": "valid", "message": "El archivo XML se ha formateado correctamente." ,"XMLformateado": pretty_xml, "fileName": file_name}, status_code=200)
        except Exception as parse_error:
            return JSONResponse(content={"status": "invalid", "fileName": file_name, "message": f"ERROR: {str(parse_error)}"}, status_code=400)
    except Exception as e:
        raise JSONResponse(content={"status": "invalid","fileName": file_name, "message": f"An error ocurred while processing the XML: {str(e)}"} ,status_code=500)

@app.post("/convertirXMLtoJSON/")
async def convert_xml_to_json(file: UploadFile, file_name: str = Form(...)):
    if file.content_type not in ["text/xml", "application/xml", "application/octet-stream"]:
        return JSONResponse(content={"status": "invalid", "message": "Invalid file type."}, status_code=400)

    try:
        contents = await file.read()

        xml_dict = xmltodict.parse(contents)
        json_data = json.dumps(xml_dict, indent=4)

        if isinstance(json.loads(json_data), dict):
            return JSONResponse(content={"status": "valid", "message": "El arhchivo XML se ha convertido a JSON correctamente.", "jsonData": json.loads(json_data), "fileName": file_name}, status_code=200)
        else:
            return JSONResponse(content={"status": "invalid", "fileName": file_name, "message": f"JSON validation failed{str(parse_error)}"}, status_code=400)
    except xmltodict.expat.ExpatError as parse_error:
        return JSONResponse(content={"status": "invalid", "fileName": file_name, "message": f"ERROR: {str(parse_error)}"}, status_code=400)
    except Exception as e:
        return JSONResponse(content={"status": "invalid", "fileName": file_name, "message": f"An error ocurred while processing the XML: {str(e)}"}, status_code=500)

@app.post("/guardarJSON/")
async def guardarXMLtoMongoDB(json_file: UploadFile):
    if json_file.content_type != "application/json":
        return JSONResponse(content={"status": "invalid", "message": "Invalid file type."}, status_code=400)

    try:
        contents = await json_file.read()

        json_data1 = json.loads(contents)

        json_data = json.loads(json_data1)

        if not isinstance(json_data, dict):
            return JSONResponse(content={"status": "invalid", "message": "JSON data must be an object.", "json_data": json_data}, status_code=400)

        result = await collection1.insert_one(json_data)
        if result.inserted_id:
            return JSONResponse(content={"status": "valid", "message": "JSON successfully saved to MongoDB."}, status_code=200)
        else:
            return JSONResponse(content={"status": "invalid", "message": "Failed to save JSON to MongoDB."}, status_code=500)
    except json.JSONDecodeError as json_error:
        return JSONResponse(content={"status": "invalid", "message": f"JSON parsing error: {str(json_error)}"}, status_code=400)
    except Exception as e:
        return JSONResponse(content={"status": "invalid", "message": f"{str(e)}"}, status_code=500)

@app.post("/validarXML/")
async def validarXML(file: UploadFile):
    if file.content_type not in ["text/xml", "application/xml", "application/octet-stream"]:
        return JSONResponse(content={"status": "invalid", "message": "Invalid file type. Please upload an XML or DATA file."}, status_code=400)
    
    try:
        contents = await file.read()
        
        try:
            xml.dom.minidom.parseString(contents)
            return JSONResponse(content={"status": "valid", "message": "XML is well-formed."}, status_code=200)
        except Exception as parse_error:
            return JSONResponse(content={"status": "invalid", "message": f"ERROR: {str(parse_error)}"}, status_code=400)
    except Exception as e:
        return JSONResponse(content={"status": "invalid", "message": f"An error occurred while processing the XML: {str(e)}"}, status_code=500)

@app.post("/validarJSON/")
async def validarJSON(json_file: UploadFile):
    print("JSON: ", json_file)
    if json_file.content_type != "application/json":
        return JSONResponse(content={"status": "invalid", "message": "Invalid file type. Please upload a JSON file."}, status_code=400)
    
    try:
        contents = await json_file.read()
        json_data0 = json.loads(contents)
        json_data1 = json.loads(json_data0)
        
        if not isinstance(json_data1, dict):
            return JSONResponse(content={"status": "invalid", "message": "JSON data must be an object."}, status_code=400)
        
        return JSONResponse(content={"status": "valid", "message": "JSON is well-formed."}, status_code=200)
    except json.JSONDecodeError as json_error:
        return JSONResponse(content={"status": "invalid", "message": f"JSON parsing error: {str(json_error)}"}, status_code=400)
    except Exception as e:
        return JSONResponse(content={"status": "invalid", "message": f"An error occurred while validating the JSON: {str(e)}"}, status_code=500)

def process_extracted_file(args):
    member, file_path, index = args
    with open(file_path, 'r', encoding='utf-8') as extracted_file:
        file_content = extracted_file.read()
    try:
        if member.endswith(".DATA"):
            xml_dom = xml.dom.minidom.parseString(file_content)
            pretty_content = xml_dom.toprettyxml()
            file_type = "application/xml"
        else:
            pretty_content = file_content
            mime = magic.Magic(mime=True)
            file_type = mime.from_buffer(file_content.encode('utf-8'))
    except Exception:
        pretty_content = file_content
        mime = magic.Magic(mime=True)
        file_type = mime.from_buffer(file_content.encode('utf-8'))
    
    file_info = {
        "filename": member,
        "content": pretty_content,
        "size": os.path.getsize(file_path),
        "type": file_type,
        "mtime": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(os.path.getmtime(file_path))),
        "id": index
    }
    return file_info

@app.post("/procesarTAR/")
async def upload_tar(file: UploadFile = File(...)):
    extracted_files = []

    tar_info = {
        "filename": file.filename,
        "size": file.size,
        "mtime": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time())),
        "type": "application/x-tar",
        "num_files": 0
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        with tarfile.open(fileobj=file.file, mode="r:gz") as tar:
            tar_members = [member for member in tar.getmembers() if member.isfile()]
            tar_info["num_files"] = len(tar_members)
            tar.extractall(path=temp_dir, members=tar_members)

        args = [
            (member.name, os.path.join(temp_dir, member.name), index)
            for index, member in enumerate(tar_members, start=1)
        ]

        with Pool(cpu_count()) as pool:
            extracted_files = pool.map(process_extracted_file, args)

    response_data = {
        "tar_info": tar_info,
        "extracted_files": extracted_files
    }
    return JSONResponse(content=response_data, status_code=200)

class FileData(BaseModel):
    filename: str
    content: str
    size: int
    type: str
    id: Optional[int] = None
    mtime: str

@app.post("/guardarListaArchivos/")
async def guardar_archivos_xml(archivos: List[FileData]):
    try:
        archivos_xml = [archivo for archivo in archivos if archivo.type == 'application/xml']

        if not archivos_xml:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "invalid",
                    "message": "No se encontraron archivos XML para guardar.",
                }
            )

        documentos = []
        for archivo in archivos_xml:
            data = archivo.dict()
            data.pop('id', None)

            try:
                contenido_json = xmltodict.parse(data['content'])
                data['content'] = contenido_json
            except Exception as e:
                return JSONResponse(
                    status_code=400,
                    content={
                        "status": "invalid",
                        "message": f"Error al convertir el contenido XML a JSON: {str(e)}",
                    }
                )

            documentos.append(data)

        resultado = await collection2.insert_many(documentos)

        return JSONResponse(
            status_code=200,
            content={
                "status": "valid",
                "message": f"Archivos XML guardados exitosamente: {len(resultado.inserted_ids)}",
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "invalid",
                "message": f"Error al guardar los archivos: {str(e)}",
            }
        )

class FileDataResponse(BaseModel):
    id: str = Field(..., alias='_id')
    filename: str
    size: int
    type: str
    mtime: str

    class Config:
        allow_population_by_field_name = True

class ObtenerArchivosResponse(BaseModel):
    status: str
    message: str
    status_code: int
    data: List[FileDataResponse]

@app.get("/obtener_datos_automatizacion", response_model=ObtenerArchivosResponse)
async def obtener_archivos(skip: int = 0, limit: int = 5):
    try:
        total_archivos = await collection2.count_documents({})

        if skip >= total_archivos:
            return {
                "status": "valid",
                "message": "No hay más archivos para mostrar",
                "status_code": 200,
                "data": []
            }

        cursor = collection2.find({}, {"content": 0}).skip(skip).limit(limit)
        documentos = await cursor.to_list(length=limit)

        for documento in documentos:
            documento['_id'] = str(documento['_id'])

        return {
            "status": "valid",
            "message": "Archivos obtenidos exitosamente",
            "status_code": 200,
            "data": documentos
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "invalid",
                "message": f"Error al obtener los archivos: {str(e)}",
                "status_code": 500,
                "data": []
            }
        )

class ContentDataResponse(BaseModel):
    id: str = Field(..., alias='_id')
    size: int
    insertion_date: str

    class Config:
        allow_population_by_field_name = True

class ObtenerContenidoResponse(BaseModel):
    status: str
    message: str
    status_code: int
    data: List[ContentDataResponse]

@app.get("/obtener_contenido_individual", response_model=ObtenerContenidoResponse)
async def obtener_contenido(skip: int = 0, limit: int = 5):
    try:
        total_contenido = await collection1.count_documents({})

        if skip >= total_contenido:
            return {
                "status": "valid",
                "message": "No hay más contenido para mostrar",
                "status_code": 200,
                "data": []
            }

        cursor = collection1.find().skip(skip).limit(limit)
        documentos = await cursor.to_list(length=limit)

        for documento in documentos:
            documento['_id'] = str(documento['_id'])

            if 'content' in documento:
                if isinstance(documento['content'], dict):
                    contenido_str = json.dumps(documento['content'])
                    documento['size'] = len(contenido_str.encode('utf-8'))
                elif isinstance(documento['content'], str):
                    documento['size'] = len(documento['content'].encode('utf-8'))
                else:
                    documento['size'] = len(str(documento['content']).encode('utf-8'))
            else:
                documento['size'] = 0

            if '_id' in documento:
                timestamp = ObjectId(documento['_id']).generation_time
                documento['insertion_date'] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            else:
                documento['insertion_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

            documento.pop('content', None)

        return {
            "status": "valid",
            "message": "Contenido obtenido exitosamente",
            "status_code": 200,
            "data": documentos
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "invalid",
                "message": f"Error al obtener el contenido: {str(e)}",
                "status_code": 500,
                "data": []
            }
        )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)