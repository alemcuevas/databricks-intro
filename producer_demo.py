"""
Demo de Productor de Event Hubs
Envía eventos de telemetría al Event Hub
"""
import asyncio
import json
import random
from datetime import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.identity.aio import DefaultAzureCredential

# Event Hub configuration
FULLY_QUALIFIED_NAMESPACE = "eh-demo-202605291122.servicebus.windows.net"
EVENTHUB_NAME = "telemetry"

async def send_events():
    """Envía eventos de telemetría al Event Hub"""
    credential = DefaultAzureCredential()
    producer = EventHubProducerClient(
        fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENTHUB_NAME,
        credential=credential
    )
    
    async with producer:
        print(f"✅ Conectado a Event Hub: {EVENTHUB_NAME}")
        print(f"📤 Enviando 100 eventos de telemetría...\n")
        
        event_data_batch = await producer.create_batch()
        
        for i in range(100):
            # Generar datos de telemetría simulados
            device_id = f"device-{random.randint(1, 10)}"
            telemetry_data = {
                "device_id": device_id,
                "temperature": round(random.uniform(15.0, 35.0), 2),
                "humidity": round(random.uniform(30.0, 80.0), 2),
                "pressure": round(random.uniform(980.0, 1020.0), 2),
                "timestamp": datetime.utcnow().isoformat()
            }
            
            event_data = EventData(json.dumps(telemetry_data))
            
            # Usar device_id como partition key para agrupar eventos del mismo dispositivo
            event_data_batch.add(event_data)
            
            if (i + 1) % 10 == 0:
                # Enviar batch cada 10 eventos
                await producer.send_batch(event_data_batch)
                print(f"📨 Enviados {i + 1} eventos...")
                event_data_batch = await producer.create_batch()
                await asyncio.sleep(0.5)  # Pequeña pausa entre batches
        
        # Enviar cualquier evento restante
        if len(event_data_batch) > 0:
            await producer.send_batch(event_data_batch)
        
        print(f"\n✅ Completado! 100 eventos enviados exitosamente")

if __name__ == "__main__":
    asyncio.run(send_events())
