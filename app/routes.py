from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from .database import get_db
from .models import Alcaldia, Stop, VehicleStatus

router = APIRouter()

@router.get("/unidades/")
def obtener_unidades(db: Session = Depends(get_db)):
    unidades = db.query(VehicleStatus).filter(VehicleStatus.route_id.isnot(None)).all()
    return [
        {
            "vehicle_id": u.vehicle_id,
            "latitude": u.latitude,
            "longitude": u.longitude,
            "route_id": u.route_id
        }
        for u in unidades
    ]

@router.get("/unidades/{vehicle_id}/ubicaciones")
def consultar_ubicacion(vehicle_id: str, db: Session = Depends(get_db)):
    unidad = db.query(VehicleStatus).filter(VehicleStatus.vehicle_id == vehicle_id).first()
    if not unidad:
        raise HTTPException(status_code=404, detail="Unidad no encontrada")
    return {
        "vehicle_id": unidad.vehicle_id,
        "latitude": unidad.latitude,
        "longitude": unidad.longitude,
        "route_id": unidad.route_id
    }

@router.get("/alcaldias/")
def obtener_alcaldias(db: Session = Depends(get_db)):
    alcaldias = db.query(VehicleStatus.alcaldia).distinct().all()
    return [{"alcaldia": a[0]} for a in alcaldias]

@router.get("/paradas/{alcaldia_nombre}")
def obtener_paradas_por_alcaldia(alcaldia_nombre: str, db: Session = Depends(get_db)):
    alcaldia = db.query(Alcaldia).filter(Alcaldia.alcaldia_nombre == alcaldia_nombre).first()
    if not alcaldia:
        raise HTTPException(status_code=404, detail="Alcaldía no encontrada")

    paradas = db.query(Stop).filter(Stop.alcaldia_id == alcaldia.alcaldia_id).all()
    if not paradas:
        raise HTTPException(status_code=404, detail="No se encontraron paradas para esta alcaldía")

    return [
        {
            "stop_id": p.stop_id,
            "stop_name": p.stop_name,
            "stop_lat": p.stop_lat,
            "stop_lon": p.stop_lon
        }
        for p in paradas
    ]