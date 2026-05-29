"""
Demo de Consumer de Event Hubs con Métricas
Consume eventos de telemetría y muestra estadísticas en tiempo real
"""
import asyncio
import json
from datetime import datetime
from collections import defaultdict
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from azure.identity.aio import DefaultAzureCredential

# Event Hub configuration
FULLY_QUALIFIED_NAMESPACE = "eh-demo-202605291122.servicebus.windows.net"
EVENTHUB_NAME = "telemetry"
STORAGE_ACCOUNT_URL = "https://checkpoint202605291122.blob.core.windows.net"
BLOB_CONTAINER_NAME = "checkpoints"
CONSUMER_GROUP = "$Default"

# Métricas globales
metrics = {
    "total_events": 0,
    "events_by_device": defaultdict(int),
    "temperature_sum": 0.0,
    "temperature_count": 0,
    "start_time": datetime.utcnow()
}

async def on_event(partition_context, event):
    """Procesa cada evento recibido"""
    try:
        # Decodificar el evento
        event_body = json.loads(event.body_as_str())
        
        # Actualizar métricas
        metrics["total_events"] += 1
        device_id = event_body.get("device_id", "unknown")
        metrics["events_by_device"][device_id] += 1
        
        temperature = event_body.get("temperature", 0)
        metrics["temperature_sum"] += temperature
        metrics["temperature_count"] += 1
        
        # Mostrar evento
        print(f"\n📩 Evento #{metrics['total_events']} - Partición {partition_context.partition_id}")
        print(f"   Device: {device_id}")
        print(f"   Temperatura: {temperature}°C")
        print(f"   Humedad: {event_body.get('humidity', 0)}%")
        print(f"   Presión: {event_body.get('pressure', 0)} hPa")
        
        # Mostrar estadísticas cada 10 eventos
        if metrics["total_events"] % 10 == 0:
            print_statistics()
        
        # Hacer checkpoint cada 10 eventos
        if metrics["total_events"] % 10 == 0:
            await partition_context.update_checkpoint(event)
            
    except Exception as e:
        print(f"❌ Error procesando evento: {e}")

def print_statistics():
    """Muestra estadísticas acumuladas"""
    elapsed = (datetime.utcnow() - metrics["start_time"]).total_seconds()
    avg_temp = metrics["temperature_sum"] / metrics["temperature_count"] if metrics["temperature_count"] > 0 else 0
    throughput = metrics["total_events"] / elapsed if elapsed > 0 else 0
    
    print("\n" + "="*60)
    print("📊 ESTADÍSTICAS")
    print("="*60)
    print(f"Total eventos procesados: {metrics['total_events']}")
    print(f"Temperatura promedio: {avg_temp:.2f}°C")
    print(f"Throughput: {throughput:.2f} eventos/segundo")
    print(f"Tiempo transcurrido: {elapsed:.1f} segundos")
    print("\nEventos por dispositivo:")
    for device, count in sorted(metrics["events_by_device"].items()):
        print(f"  {device}: {count} eventos")
    print("="*60 + "\n")

async def main():
    """Función principal del consumer"""
    credential = DefaultAzureCredential()
    
    # Crear checkpoint store
    checkpoint_store = BlobCheckpointStore(
        blob_account_url=STORAGE_ACCOUNT_URL,
        container_name=BLOB_CONTAINER_NAME,
        credential=credential
    )
    
    # Crear consumer client
    client = EventHubConsumerClient(
        fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENTHUB_NAME,
        consumer_group=CONSUMER_GROUP,
        checkpoint_store=checkpoint_store,
        credential=credential
    )
    
    print(f"✅ Conectado a Event Hub: {EVENTHUB_NAME}")
    print(f"🎧 Escuchando eventos del consumer group: {CONSUMER_GROUP}")
    print(f"💾 Checkpoints en: {BLOB_CONTAINER_NAME}")
    print(f"⏳ Esperando eventos... (Presiona Ctrl+C para detener)\n")
    
    try:
        async with client:
            await client.receive(
                on_event=on_event,
                starting_position="-1"  # Comenzar desde el final (solo nuevos eventos)
            )
    except KeyboardInterrupt:
        print("\n\n🛑 Consumer detenido por el usuario")
        print_statistics()
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
