# main.py - FastAPI Backend Mejorado
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks, Depends, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import pandas as pd
import tempfile
import os
from typing import Optional, Dict, Any, List
import uuid
from datetime import datetime, timedelta
import logging
from pathlib import Path
import shutil
import asyncio
from contextlib import asynccontextmanager
import json
import re

# Importar tu clase existente
from shipment_generator_v2 import ShipmentXMLGenerator, generate_validated_plates_excel

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Modelos Pydantic
class ProcessingOptions(BaseModel):
    use_planta_as_origen: bool = False
    skip_placas: bool = False


class JobResponse(BaseModel):
    job_id: str
    status: str
    progress: int = 0
    message: str = ""
    result_files: List[str] = []
    error: Optional[str] = None
    validation_stats: Optional[Dict[str, Any]] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    file_type: Optional[str] = None
    file_date: Optional[str] = None


class DatabaseConfig(BaseModel):
    host: str = "localhost"
    database: str = "tms_shipment_db"
    user: str = "root"
    password: str = "Dali19((Kafka"


# Estado global de trabajos
jobs_storage: Dict[str, JobResponse] = {}


# Configuración de la aplicación
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("🚀 Iniciando Shipment XML Generator API...")

    # Crear directorios necesarios
    os.makedirs("uploads", exist_ok=True)
    os.makedirs("outputs", exist_ok=True)
    os.makedirs("temp", exist_ok=True)

    logger.info("✅ Directorios creados")

    yield

    # Shutdown
    logger.info("🛑 Cerrando Shipment XML Generator API...")

    # Limpiar archivos temporales
    cleanup_temp_files()


def cleanup_temp_files():
    """Limpiar archivos temporales antiguos"""
    try:
        temp_dir = Path("temp")
        if temp_dir.exists():
            for file_path in temp_dir.iterdir():
                if file_path.is_file():
                    # Eliminar archivos de más de 1 hora
                    if (datetime.now().timestamp() - file_path.stat().st_mtime) > 3600:
                        file_path.unlink()
                        logger.info(f"🗑️ Archivo temporal eliminado: {file_path}")
    except Exception as e:
        logger.error(f"Error limpiando archivos temporales: {e}")


def extract_date_from_filename(file_path: str) -> tuple:
    """
    Extraer día y mes del nombre del archivo.
    Lógica copiada de shipment_generator_v2.py para ser usada en la API.
    """
    try:
        file_name = os.path.basename(file_path)

        # PATRÓN 1: SD - Programa_SD_D_MM_YYYY_
        pattern_sd = r'Programa_SD_(\d{1,2})_(\d{1,2})_\d{4}_'
        match_sd = re.search(pattern_sd, file_name, re.IGNORECASE)
        if match_sd:
            day = match_sd.group(1).zfill(2)
            month = match_sd.group(2).zfill(2)
            return month, day

        # PATRÓN 2: CB - Envíos CBs DD-MM
        pattern_cb = r'Envíos\s+CBs?\s+(\d{1,2})-(\d{1,2})'
        match_cb = re.search(pattern_cb, file_name, re.IGNORECASE)
        if match_cb:
            day = match_cb.group(1).zfill(2)
            month = match_cb.group(2).zfill(2)
            return month, day

        # PATRÓN 3: Genérico - DD_MM
        pattern_generic = r'(\d{1,2})_(\d{1,2})'
        match_generic = re.search(pattern_generic, file_name)
        if match_generic:
            day = match_generic.group(1).zfill(2)
            month = match_generic.group(2).zfill(2)
            return month, day

        # Si no se encuentra ningún patrón, devuelve None
        logger.warning(f"⚠️ No se pudo extraer fecha de: {file_name}")
        return None, None
    except Exception as e:
        logger.error(f"Error extrayendo fecha del nombre del archivo: {str(e)}")
        return None, None

# Crear aplicación FastAPI
app = FastAPI(
    title="Shipment XML Generator API",
    description="API para generar XML de envíos con integración de base de datos",
    version="5.0.0",
    lifespan=lifespan
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Servir archivos estáticos (para downloads)
app.mount("/static", StaticFiles(directory="outputs"), name="static")


# Dependency para configuración de DB
def get_db_config() -> DatabaseConfig:
    return DatabaseConfig()


class JobManager:
    """Gestor de trabajos con persistencia en memoria"""

    @staticmethod
    def create_job() -> str:
        job_id = str(uuid.uuid4())
        jobs_storage[job_id] = JobResponse(
            job_id=job_id,
            status="pending",
            message="Trabajo creado",
            started_at=datetime.now()
        )
        return job_id

    @staticmethod
    def update_job(job_id: str, **kwargs):
        if job_id in jobs_storage:
            job = jobs_storage[job_id]
            for key, value in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, value)

            if kwargs.get('status') == 'completed':
                job.completed_at = datetime.now()

    @staticmethod
    def get_job(job_id: str) -> Optional[JobResponse]:
        return jobs_storage.get(job_id)


# Endpoints de la API

@app.get("/")
async def root():
    """Endpoint raíz con información de la API"""
    return {
        "name": "Shipment XML Generator API",
        "version": "5.0.0",
        "status": "running",
        "timestamp": datetime.now(),
        "endpoints": {
            "upload": "/api/upload-file",
            "job_status": "/api/job/{job_id}",
            "download": "/api/download/{file_path}",
            "health": "/api/health"
        }
    }


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Verificar conexión a base de datos
        db_config = get_db_config()

        return {
            "status": "healthy",
            "timestamp": datetime.now(),
            "database": {
                "host": db_config.host,
                "database": db_config.database,
                "status": "connected"  # Aquí podrías hacer una verificación real
            },
            "active_jobs": len([j for j in jobs_storage.values() if j.status == "processing"]),
            "total_jobs": len(jobs_storage)
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")


@app.post("/api/upload-file")
async def upload_file(
        background_tasks: BackgroundTasks,
        file: UploadFile = File(...),
        availability_file: UploadFile = File(None),
        # 2. Cambia la forma en que se reciben estos parámetros
        use_planta_as_origen: str = Form("false"),
        skip_placas: str = Form("false"),
        db_config: DatabaseConfig = Depends(get_db_config)
):
    """
    Subir archivo Excel y procesarlo
    """
    logger.info(f"📤 Subiendo archivo: {file.filename}")

    use_planta_bool = use_planta_as_origen.lower() == 'true'
    skip_placas_bool = skip_placas.lower() == 'true'

    # Validar tipo de archivo
    if not file.filename.lower().endswith(('.xlsx', '.xlsm', '.xls')):
        raise HTTPException(
            status_code=400,
            detail="Solo se permiten archivos Excel (.xlsx, .xlsm, .xls)"
        )

    # Validar tamaño de archivo (máximo 50MB)
    if file.size and file.size > 50 * 1024 * 1024:
        raise HTTPException(
            status_code=400,
            detail="El archivo es demasiado grande (máximo 50MB)"
        )

    availability_file_path = None
    if availability_file:
        # Si se sube, lo guardamos en la carpeta correcta
        month, day = extract_date_from_filename(file.filename)  # Reutilizamos la función
        if month and day:
            availability_dir = Path("disponibilidad_camiones") / month
            availability_dir.mkdir(parents=True, exist_ok=True)

            # Nombre estándar que el script espera
            standard_name = f"Disponibilidad de Camiones {day}-{month}.xlsx"
            availability_file_path = availability_dir / standard_name

            with open(availability_file_path, "wb") as buffer:
                content = await availability_file.read()
                buffer.write(content)

            logger.info(f"✅ Archivo de disponibilidad guardado en: {availability_file_path}")

    # Crear trabajo
    job_id = JobManager.create_job()

    # Guardar archivo temporalmente
    temp_dir = Path("temp") / job_id
    temp_dir.mkdir(exist_ok=True)
    temp_file = temp_dir / file.filename

    try:
        # Escribir archivo
        with open(temp_file, "wb") as buffer:
            content = await file.read()
            buffer.write(content)

        logger.info(f"✅ Archivo guardado: {temp_file}")

        # Procesar en background
        background_tasks.add_task(
            process_shipment_file,
            job_id,
            str(temp_file),
            use_planta_bool,  # <-- Usar la nueva variable
            skip_placas_bool,  # <-- Usar la nueva variable
            db_config.dict(),
            availability_provided=(availability_file_path is not None)
        )

        return {
            "job_id": job_id,
            "status": "processing",
            "message": "Archivo subido, procesamiento iniciado",
            "filename": file.filename
        }

    except Exception as e:
        logger.error(f"❌ Error subiendo archivo: {e}")
        JobManager.update_job(job_id, status="error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Error procesando archivo: {str(e)}")


async def process_shipment_file(
        job_id: str,
        file_path: str,
        use_planta: bool,
        skip_placas: bool,
        db_config: dict,
        availability_provided: bool
):
    """
    Procesar archivo de envíos en background
    """

    # Extraer la fecha del nombre del archivo original
    month, day = extract_date_from_filename(file_path)
    file_date_str = None
    if month and day:
        # Asumimos el año actual si no se especifica en el nombre del archivo.
        # Podríamos hacer esto más inteligente si el formato del nombre siempre incluye el año.
        current_year = datetime.now().year
        file_date_str = f"{current_year}-{month}-{day}"

    # Guardamos la fecha extraída en el job para que el frontend pueda usarla
    JobManager.update_job(job_id, file_date=file_date_str)
    # ==================== FIN DE LA MODIFICACIÓN =====================

    logger.info(f"🔄 Iniciando procesamiento de trabajo: {job_id}")

    file_name_upper = Path(file_path).name.upper()
    file_type = "general"
    if "BEER" in file_name_upper:
        file_type = "beer"
    elif "SD" in file_name_upper:
        file_type = "sd"
    elif "CB" in file_name_upper:
        file_type = "cb"

    try:
        # Actualizar estado inicial
        JobManager.update_job(
            job_id,
            status="processing",
            progress=5,
            message="Verificando archivo...",
            file_type=file_type
        )

        # Verificar que el archivo existe
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")

        # Validar archivo Excel
        JobManager.update_job(job_id, progress=10, message="Validando archivo Excel...")

        try:
            # Verificar que se puede leer el archivo
            df_test = pd.read_excel(file_path, sheet_name='Consolidado', nrows=1)
            logger.info(f"✅ Archivo Excel válido con {len(df_test.columns)} columnas")
        except Exception as e:
            raise ValueError(f"Error leyendo archivo Excel: {str(e)}")

        # Conectar a base de datos
        JobManager.update_job(job_id, progress=20, message="Conectando a base de datos...")

        # Crear generador con tu clase existente
        generator = ShipmentXMLGenerator(db_config, use_planta_as_origen=use_planta)

        # Procesar archivo
        JobManager.update_job(job_id, progress=40, message="Procesando datos del archivo...")

        # Crear directorio de salida para este trabajo
        output_dir = Path("outputs") / job_id
        output_dir.mkdir(exist_ok=True)

        # Generar XML usando tu método existente
        JobManager.update_job(job_id, progress=60, message="Generando archivo XML...")

        xml_file = generator.generate_xml_from_file(file_path)

        # Mover archivo XML al directorio de salida
        xml_output = output_dir / f"shipment_{job_id}.xml"
        shutil.copy2(xml_file, xml_output)

        # result_files = [str(xml_output)]
        result_files = [xml_output.as_posix()]
        # Procesar placas si no se omiten
        if not skip_placas:

            logger.info("Iniciando la generación de archivo de placas.")
            # Esta verificación no detiene el proceso, solo informa.
            if not availability_provided:
                JobManager.update_job(job_id, progress=75,
                                      message="ADVERTENCIA: No se proporcionó archivo de disponibilidad. Los resultados pueden ser incompletos.")
                await asyncio.sleep(2)  # Pausa para que el usuario vea el mensaje

            JobManager.update_job(job_id, progress=80, message="Procesando placas de disponibilidad...")

            try:
                placas_file_path = generate_validated_plates_excel(
                    file_path,
                    db_config,
                    destination_folder=str(output_dir),
                    etapa2_folder=getattr(generator, 'etapa2_folder', None)
                )

                if placas_file_path and os.path.exists(placas_file_path):
                    result_files.append(Path(placas_file_path).as_posix())
                    logger.info(f"Archivo de placas añadido a los resultados: {placas_file_path}")
                else:
                    logger.warning("La generación de placas no produjo un archivo de salida válido.")

            except Exception as e:
                logger.warning(f"⚠️ Error procesando placas: {e}")
                # No fallar todo el proceso por error en placas
        else:
            logger.info("Placas omitidas según la configuración del trabajo.")

        # Obtener estadísticas de validación
        validation_stats = generator.validation_stats if hasattr(generator, 'validation_stats') else {}

        # Completar trabajo exitosamente
        JobManager.update_job(
            job_id,
            status="completed",
            progress=100,
            message="Procesamiento completado exitosamente",
            result_files=result_files,
            validation_stats=validation_stats,
            file_type=file_type
        )

        logger.info(f"✅ Trabajo completado exitosamente: {job_id}")
        logger.info(f"📁 Archivos generados: {result_files}")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ Error en trabajo {job_id}: {error_msg}")

        JobManager.update_job(
            job_id,
            status="error",
            error=error_msg,
            message=f"Error: {error_msg}"
        )

    finally:
        # Limpiar archivo temporal
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"🗑️ Archivo temporal eliminado: {file_path}")
        except Exception as e:
            logger.warning(f"No se pudo eliminar archivo temporal: {e}")


@app.get("/api/job/{job_id}")
async def get_job_status(job_id: str):
    """
    Obtener estado de un trabajo
    """
    job = JobManager.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Trabajo no encontrado")

    return job


@app.get("/api/jobs")
async def list_jobs(limit: int = 50):
    """
    Listar todos los trabajos (para debugging)
    """
    jobs_list = list(jobs_storage.values())
    jobs_list.sort(key=lambda x: x.started_at or datetime.min, reverse=True)

    return {
        "jobs": jobs_list[:limit],
        "total": len(jobs_storage)
    }


@app.get("/api/download/{file_path:path}")
async def download_file(file_path: str):
    """
    Descargar archivo generado
    """
    # Verificar que el archivo existe y está en el directorio de outputs
    full_path = Path(file_path)

    if not full_path.exists():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    # Verificar que está en directorio seguro
    if not str(full_path.resolve()).startswith(str(Path("outputs").resolve())):
        raise HTTPException(status_code=403, detail="Acceso denegado")

    filename = full_path.name

    logger.info(f"📥 Descargando archivo: {filename}")

    return FileResponse(
        path=full_path,
        media_type='application/octet-stream',
        filename=filename
    )


@app.delete("/api/job/{job_id}")
async def delete_job(job_id: str):
    """
    Eliminar un trabajo y sus archivos
    """
    job = JobManager.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Trabajo no encontrado")

    try:
        # Eliminar archivos de resultado
        output_dir = Path("outputs") / job_id
        if output_dir.exists():
            shutil.rmtree(output_dir)

        # Eliminar del storage
        del jobs_storage[job_id]

        logger.info(f"🗑️ Trabajo eliminado: {job_id}")

        return {"message": "Trabajo eliminado exitosamente"}

    except Exception as e:
        logger.error(f"Error eliminando trabajo {job_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error eliminando trabajo: {str(e)}")


# Endpoint para limpiar trabajos antiguos
@app.post("/api/cleanup")
async def cleanup_old_jobs(days: int = 7):
    """
    Limpiar trabajos de más de X días
    """
    cutoff_date = datetime.now() - timedelta(days=days)
    cleaned_jobs = 0

    try:
        jobs_to_delete = []

        for job_id, job in jobs_storage.items():
            if job.started_at and job.started_at < cutoff_date:
                jobs_to_delete.append(job_id)

        for job_id in jobs_to_delete:
            # Eliminar archivos
            output_dir = Path("outputs") / job_id
            if output_dir.exists():
                shutil.rmtree(output_dir)

            # Eliminar del storage
            del jobs_storage[job_id]
            cleaned_jobs += 1

        logger.info(f"🧹 Limpieza completada: {cleaned_jobs} trabajos eliminados")

        return {
            "message": f"Limpieza completada",
            "jobs_cleaned": cleaned_jobs,
            "cutoff_date": cutoff_date
        }

    except Exception as e:
        logger.error(f"Error en limpieza: {e}")
        raise HTTPException(status_code=500, detail=f"Error en limpieza: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
