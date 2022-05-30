# Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE

Aplicación de segundo plano que se encarga de insertar en el grafo de búsqueda de cada usuario los triples de los mensajes que envía y recibe dentro de la plataforma.

Configuración estandar de esta aplicación en el archivo docker-compose.yml: 

```yml
socialsearchgraphgeneration:
    image: socialsearchgraphgeneration
    env_file: .env
    environment:
     virtuosoConnectionString_home: ${virtuosoConnectionString_home}
     virtuosoConnectionString: ${virtuosoConnectionString}
     acid: ${acid}
     base: ${base}
     VirtuosoHome__VirtuosoEscrituraHome: ${virtuosoConnectionString_Escriturahome}
     replicacionActivadaHome: "true"
     RabbitMQ__colaServiciosWin: ${RabbitMQ}
     RabbitMQ__colaReplicacion: ${RabbitMQ}
     redis__redis__ip__master: ${redis__redis__ip__master}
     redis__redis__bd: ${redis__redis__bd}
     redis__redis__timeout: ${redis__redis__timeout}
     redis__recursos__ip__master: ${redis__recursos__ip__master}
     redis__recursos__bd: ${redis__recursos_bd}
     redis__recursos__timeout: ${redis__recursos_timeout}
     redis__liveUsuarios__ip__master: ${redis__liveUsuarios__ip__master}
     redis__liveUsuarios__bd: ${redis__liveUsuarios_bd}
     redis__liveUsuarios__timeout: ${redis__liveUsuarios_timeout}
     idiomas: "es|Español,en|English"
     tipoBD: 0
     Servicios__urlBase: "https://servicios.test.com"
     Servicios__urlServicioEtiquetadoAutomatico: ${servicio_urlEtiquetadoAutomatico}
     connectionType: "0"
     intervalo: "100"
    volumes:
     - /home/$USER/docker_testing/logs/base_usuarios:/app/logs
```

Se pueden consultar los posibles valores de configuración de cada parámetro aquí: https://github.com/equipognoss/Gnoss.Platform.Deploy
