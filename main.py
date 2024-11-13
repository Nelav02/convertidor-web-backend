from fastapi import FastAPI, UploadFile, HTTPException
from fastapi import Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import xml.dom.minidom
import xmltodict
import json
import motor.motor_asyncio
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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)