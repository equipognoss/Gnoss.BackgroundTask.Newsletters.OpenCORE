![](https://content.gnoss.ws/imagenes/proyectos/personalizacion/7e72bf14-28b9-4beb-82f8-e32a3b49d9d3/cms/logognossazulprincipal.png)

# Gnoss.BackgroundTask.Newsletters.OpenCORE

![](https://github.com/equipognoss/Gnoss.BackgroundTask.Newsletters.OpenCORE/workflows/BuildNewsLetter/badge.svg)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE&metric=bugs)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=equipognoss_Gnoss.BackgroundTask.Newsletters.OpenCORE)

Aplicación de segundo plano que se encarga de enviar a todos los usuarios de una comunidad los emails de una newsletter.

Este servicio escucha la cola ColaNewsletter. La Web envía a esta cola un evento cada vez que un administrador envía una newsletter a todos los usuarios de la comunidad. Cuando este servicio recibe el evento, inserta en la tabla ColaCorreo una fila con el contenido de la newsletter por cada usuario al que se debe enviar la newsletter, para que sea finalmente el servicio MailService el que envíe el email de cada usuario. 

Es decir, este servicio se encarga de preparar los envíos de todos los emails a enviar, pero no realiza el envío. Una vez preparados, se los pasa al servicio MailService para que los envíe.

Configuración estandar de esta aplicación en el archivo docker-compose.yml: 

```yml
newsletters:
    image: gnoss/gnoss.backgroundtask.newsletters.opencore
    env_file: .env
    environment:
virtuosoConnectionString: ${virtuosoConnectionString}
     acid: ${acid}
     base: ${base}
     RabbitMQ__colaServiciosWin: ${RabbitMQ}
     RabbitMQ__colaReplicacion: ${RabbitMQ}
     redis__redis__ip__master: ${redis__redis__ip__master}
     redis__redis__bd: ${redis__redis__bd}
     redis__redis__timeout: ${redis__redis__timeout}
     redis__recursos__ip__master: ${redis__recursos__ip__master}
     redis__recursos__bd: ${redis__recursos_bd}
     redis__recursos__timeout: ${redis__recursos_timeout}
     idiomas: "es|Español,en|English"
     Servicios__urlBase: "https://servicios.test.com"
     connectionType: "0"
     intervalo: "100"
     ruta: ""
    volumes:
     - ./logs/newsletters:/app/logs
```

Se pueden consultar los posibles valores de configuración de cada parámetro aquí: https://github.com/equipognoss/Gnoss.SemanticAIPlatform.OpenCORE

## Código de conducta
Este proyecto a adoptado el código de conducta definido por "Contributor Covenant" para definir el comportamiento esperado en las contribuciones a este proyecto. Para más información ver https://www.contributor-covenant.org/

## Licencia
Este producto es parte de la plataforma [Gnoss Semantic AI Platform Open Core](https://github.com/equipognoss/Gnoss.SemanticAIPlatform.OpenCORE), es un producto open source y está licenciado bajo GPLv3.
