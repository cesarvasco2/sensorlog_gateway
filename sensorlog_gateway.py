import sys
import decimal
import json
import paho.mqtt.client as mqtt
import boto3
import time
from datetime import date, datetime
import calendar
import ssl
sqs = boto3.resource('sqs', region_name='us-east-1')
queue = sqs.get_queue_by_name(QueueName='processador_entrada')
#configurações do broker:
Broker = 'servermqtt.duckdns.org'
PortaBroker = 8883 
Usuario = 'afira'
Senha = 'afira'
KeepAliveBroker = 60
TopicoSubscribe = 'SENSORLOG/#' #Topico que ira se inscrever
#Callback - conexao ao broker realizada
def on_connect(client, userdata, flags, rc):
    print('[STATUS] Conectado ao Broker. Resultado de conexao: '+str(rc))

#faz subscribe automatico no topico
    client.subscribe(TopicoSubscribe)

#Callback - mensagem recebida do broker
def on_message(client, userdata, msg):
    MensagemRecebida = str(msg.payload.decode('utf-8'))
    #print("[MESAGEM RECEBIDA] Topico: "+msg.topic+" / Mensagem: "+MensagemRecebida)
    #programa principal:
    msg.topic.split('/')[2]
    lista_de_campos = [
        {'key':'id_dispositivo','type':'str','fields':'ID'},
        {'key':'distancia','type':'str','fields':('DIST',)},
        {'key':'sinal','type':'str','fields':'RSSI'},
        {'key':'sinal_ruido','type':'str','fields':'SNR'},
        {'key':'voltagem_bateria','type':'str','fields':'V1'},
        {'key':'temperatura','type':'str','fields':'T1'}
        
    ]
    payload_dict = json.loads(MensagemRecebida)
    #print(payload_dict['EVENT']['DEVICE'])
    dia_semana = date.today() 
    data_e_hora_atuais = datetime.now()
    dict_payload = {}
    dict_payload['id_dispositivo'] = str(payload_dict['EVENT']['DEVICE']['ID'])
    dict_payload['data_hora_dispositivo'] = data_e_hora_atuais.strftime('%Y-%m-%d %H:%M:%S')
    dict_payload['ultra_sonico'] = str(payload_dict['EVENT']['DEVICE']['DIST']/10)
    dict_payload['sinal'] = str(payload_dict['EVENT']['DEVICE']['RSSI'])
    dict_payload['sinal_ruido'] =   str(payload_dict['EVENT']['DEVICE']['SNR'])
    dict_payload['voltagem_bateria'] =  str(payload_dict['EVENT']['DEVICE']['V1']/1000)
    dict_payload['temperatura'] =   str(payload_dict['EVENT']['DEVICE']['T1']/100)
    dict_payload['codigo_produto'] = 20
    dict_payload['timestamp_servidor'] = int(datetime.now().timestamp())
    dict_payload['timestamp_dispositivo'] = int(datetime.now().timestamp())
    dict_payload['dia_sem'] = calendar.day_name[dia_semana.weekday()]
    print(dict_payload)
    queue.send_message(MessageBody=str(json.dumps(dict_payload, ensure_ascii=False)))

try:
    print('[STATUS] Inicializando MQTT...')
#inicializa MQTT:
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(Usuario, Senha)
    # the key steps here
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    # if you do not want to check the cert hostname, skip it
    # context.check_hostname = False
    client.tls_set_context(context)
    client.connect(Broker, PortaBroker, KeepAliveBroker)
    client.loop_forever()

except KeyboardInterrupt:
    print ('\nCtrl+C pressionado, encerrando aplicacao e saindo...')
    sys.exit(0)
    