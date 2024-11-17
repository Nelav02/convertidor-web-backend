from fastapi import FastAPI, UploadFile, HTTPException, File
from fastapi import Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import  List, Dict
from lxml import etree
from concurrent.futures import ThreadPoolExecutor
import asyncio
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
collection = database.get_collection("xml_json_collection")

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

        result = await collection.insert_one(json_data)
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

def process_extracted_file(member, extracted_file, index):
    file_content = extracted_file.read().decode('utf-8')
    try:
        if member.name.endswith(".DATA"):
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
        "filename": member.name,
        "content": pretty_content,
        "size": member.size,
        "type": file_type,
        "mtime": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(member.mtime)),
        "id": index
    }
    return file_info

@app.post("/procesarTAR/")
async def upload_tar(file: UploadFile = File(...)):
    extracted_files = []

    tar_info = {
        "filename": file.filename,
        "size": f"{file.size} bytes",
        "mtime": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time())),
        "type": "application/x-tar",
        "num_files": 0
    }

    with tarfile.open(fileobj=file.file, mode="r:gz") as tar:
        tar_members = [member for member in tar.getmembers() if member.isfile()]
        tar_info["num_files"] = len(tar_members)

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            tasks = []
            for index, member in enumerate(tar_members, start=1):
                extracted_file = tar.extractfile(member)
                if extracted_file:
                    tasks.append(loop.run_in_executor(executor, process_extracted_file, member, extracted_file, index))
            extracted_files = await asyncio.gather(*tasks)

    response_data = {
        "tar_info": tar_info,
        "extracted_files": extracted_files
    }
    return JSONResponse(content=response_data, status_code=200)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)