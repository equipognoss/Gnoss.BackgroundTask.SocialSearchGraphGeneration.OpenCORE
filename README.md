![](https://content.gnoss.ws/imagenes/proyectos/personalizacion/7e72bf14-28b9-4beb-82f8-e32a3b49d9d3/cms/logognossazulprincipal.png)


# Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE

![](https://github.com/equipognoss/Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE/workflows/BuildSocialSearchGraphGeneration/badge.svg)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE&metric=bugs)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.SocialSearchGraphGeneration.OpenCORE)

Aplicación de segundo plano que se encarga de insertar en el grafo de búsqueda de cada usuario los triples de los mensajes que envía y recibe dentro de la plataforma.

Este servicio está escuchando las siguientes colas: 

* ColaTagsMensaje: En esta cola la Web notificará el envío de cualquier mensaje interno a través de la plataforma. 
* ColaTagsComentarios: En esta cola la Web notificará la realización de un comentario al recurso de un usuario. 

Configuración estandar de esta aplicación en el archivo docker-compose.yml: 

```yml
socialsearchgraphgeneration:
    image: gnoss/gnoss.backgroundtask.socialsearchgraphgeneration.opencore
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
     - ./logs/socialsearchgraphgeneration:/app/logs
```

Se pueden consultar los posibles valores de configuración de cada parámetro aquí: https://github.com/equipognoss/Gnoss.SemanticAIPlatform.OpenCORE

## Código de conducta
Este proyecto a adoptado el código de conducta definido por "Contributor Covenant" para definir el comportamiento esperado en las contribuciones a este proyecto. Para más información ver https://www.contributor-covenant.org/

## Licencia
Este producto es parte de la plataforma [Gnoss Semantic AI Platform Open Core](https://github.com/equipognoss/Gnoss.SemanticAIPlatform.OpenCORE), es un producto open source y está licenciado bajo GPLv3. 

