using System;
using System.Collections.Generic;
using System.Text;
using Es.Riam.Gnoss.Util.General;
using System.IO;
using Es.Riam.Gnoss.Logica.Notificacion;
using Es.Riam.Gnoss.Elementos.Notificacion;
using System.Threading;
using System.Reflection;
using System.Globalization;
using Es.Riam.Gnoss.AD.Notificacion;
using Es.Riam.Util;
using System.Net.Mail;
using Es.Riam.Gnoss.AD.ServiciosGenerales;
using Es.Riam.Gnoss.Logica.ParametroAplicacion;
using Es.Riam.Gnoss.Logica.ServiciosGenerales;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Recursos;
using Es.Riam.Gnoss.Logica.Parametro;
using Es.Riam.Gnoss.Elementos.Documentacion;
using Es.Riam.Gnoss.Logica.Documentacion;
using Es.Riam.Gnoss.Logica.Identidad;
using System.Data;
using Es.Riam.Gnoss.Logica.BASE_BD;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using System.Linq;
using Es.Riam.Gnoss.Elementos.ParametroGeneralDSEspacio;
using Es.Riam.Gnoss.Web.Controles.ParametroGeneralDSName;
using Es.Riam.Gnoss.AD.EntityModel.Models.ProyectoDS;
using Es.Riam.Gnoss.AD.EncapsuladoDatos;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.RabbitMQ;
using System.Diagnostics;
using Newtonsoft.Json;
using Es.Riam.Gnoss.AD.EntityModel.Models;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.CL;
using Es.Riam.AbstractsOpen;
using Es.Riam.Interfaces.InterfacesOpen;
using Microsoft.Extensions.Logging;
using Es.Riam.Gnoss.Elementos.Suscripcion;

namespace Es.Riam.Gnoss.Win.ServicioEnviosMasivos
{
    public class Controller : ControladorServicioGnoss
    {
        #region Constantes

        private readonly string COLA_NEWSLETTER = "ColaNewsletter";
        private readonly string EXCHANGE = "";

        #endregion

        #region Estaticos

        private static object OBJETO_BLOQUEO_ENVIO_NEWSLETTER = new object();

        #endregion

        #region Miembros

        /// <summary>
        /// Gestor de notificaciones
        /// </summary>
        private GestionNotificaciones mGestionNotificaciones;

        /// <summary>
        /// Lista de DocumentoNewsletter
        /// </summary>
        private List<DocumentoNewsletter> mListaNewsletter;

        private readonly Dictionary<Guid, ConfiguracionEnvioCorreo> mListaConfiguracionEnvioCorreo;

        private ILogger<Controller> mlogger;
        private ILoggerFactory mLoggerFactory;

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor a partir de la base de datos pasada por parmetro
        /// </summary>
        /// <param name="pBaseDeDatos">Base de datos</param>
        public Controller(IServiceScopeFactory scopedFactory, ConfigService configService, ILogger<Controller> logger, ILoggerFactory loggerFactory)
            : base(scopedFactory, configService,logger,loggerFactory)
        {
            mListaConfiguracionEnvioCorreo = new Dictionary<Guid, ConfiguracionEnvioCorreo>();
            mlogger = logger;
            mLoggerFactory = loggerFactory;
        }

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new Controller(ScopedFactory, mConfigService, mLoggerFactory.CreateLogger<Controller>(), mLoggerFactory);
        }

        #endregion

        #region M�todos

        #region P�blicos

        /// <summary>
        /// Realiza el envio de las notificaciones pendientes de enviar 
        /// y escribe en el log una entrada indicando el resultado de la operaci�n
        /// </summary>
        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            RealizarMantenimientoRabbitMQ(loggingService);
            RealizarMantenimientoBD();
        }

        #endregion

        #region Privados

        private void RealizarMantenimientoBD()
        {
            while (true)
            {
                using (var scope = ScopedFactory.CreateScope())
                {
                    EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                    EntityContextBASE entityContextBASE = scope.ServiceProvider.GetRequiredService<EntityContextBASE>();
                    LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                    IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                    IAvailableServices availableServices = scope.ServiceProvider.GetRequiredService<IAvailableServices>();
                    lock (OBJETO_BLOQUEO_ENVIO_NEWSLETTER)
                    {
                        try
                        {
                            ComprobarCancelacionHilo();

                            if (mReiniciarLecturaRabbit)
                            {
                                RealizarMantenimientoRabbitMQ(loggingService);
                            }

                            //(Re)Carga los datos de la BD referentes a notificaciones
                            CargarDatos(entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                            //Envío de notificaciones
                            EnviarNewsLetter(entityContext, loggingService, entityContextBASE, servicesUtilVirtuosoAndReplication, availableServices);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch (Exception ex)
                        {
                            loggingService.GuardarLog($"{LogStatus.Error.ToString().ToUpper()} ({mFicheroConfiguracionBD}) {CrearEntradaRegistro(LogStatus.Error, ex.Message)}",mlogger);
                        }
                        finally
                        {
                            //Dormimos el proceso el tiempo establecido
                            Thread.Sleep(INTERVALO_SEGUNDOS * 1000);
                        }
                    }
                }
            }
        }

        private void RealizarMantenimientoRabbitMQ(LoggingService loggingService)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItem);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);

                RabbitMQClient rabbitMQClient = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_NEWSLETTER, loggingService, mConfigService, mLoggerFactory.CreateLogger<RabbitMQClient>(), mLoggerFactory, EXCHANGE, COLA_NEWSLETTER);

                try
                {
                    rabbitMQClient.ObtenerElementosDeCola(funcionProcesarItem, funcionShutDown);
                    mReiniciarLecturaRabbit = false;
                }
                catch (Exception ex)
                {
                    mReiniciarLecturaRabbit = true;
                    loggingService.GuardarLogError(ex,mlogger);
                }
            }
        }

        private bool ProcesarItem(string pFila)
        {
            using (var scope = ScopedFactory.CreateScope())
            {
                EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                EntityContextBASE entityContextBASE = scope.ServiceProvider.GetRequiredService<EntityContextBASE>();
                LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                RedisCacheWrapper redisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                ConfigService configService = scope.ServiceProvider.GetRequiredService<ConfigService>();
                IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                IAvailableServices availableServices = scope.ServiceProvider.GetRequiredService<IAvailableServices>();
                ComprobarTraza("Newsletters", entityContext, loggingService, redisCacheWrapper, configService, servicesUtilVirtuosoAndReplication);
                try
                {
                    ComprobarCancelacionHilo();

                    Debug.WriteLine($"ProcesarItem, {pFila}!");

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        AD.EntityModel.Models.Documentacion.DocumentoEnvioNewsLetter documentoEnvioNewsLetter = JsonConvert.DeserializeObject<AD.EntityModel.Models.Documentacion.DocumentoEnvioNewsLetter>(pFila);

                        ProcesarFilaDeCola(documentoEnvioNewsLetter, loggingService, entityContext, entityContextBASE, servicesUtilVirtuosoAndReplication, availableServices);

                        ControladorConexiones.CerrarConexiones(false);
                    }

                    return true;
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLogError(ex, mlogger);
                    return true;
                }
                finally
                {
                    GuardarTraza(loggingService);
                }
            }
        }

        private void ProcesarFilaDeCola(AD.EntityModel.Models.Documentacion.DocumentoEnvioNewsLetter pDocumentoEnvioNewsLetter, LoggingService loggingService, EntityContext entityContext, EntityContextBASE entityContextBASE, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication, IAvailableServices availableServices)
        {
            lock (OBJETO_BLOQUEO_ENVIO_NEWSLETTER)
            {
                try
                {
                    ComprobarCancelacionHilo();
                    //(Re)Carga los datos de la BD referentes a notificaciones
                    CargarDatosRabbit(pDocumentoEnvioNewsLetter, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                    EnviarNewsLetter(entityContext, loggingService, entityContextBASE, servicesUtilVirtuosoAndReplication, availableServices, true);
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLog($"{LogStatus.Error.ToString().ToUpper()} ({mFicheroConfiguracionBD}) {CrearEntradaRegistro(LogStatus.Error, ex.Message)}", mlogger);
                }
            }
        }

        private ConfiguracionEnvioCorreo ObtenerParametrosConfiguracionEnvioCorreo(Guid pProyectoID, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ConfiguracionEnvioCorreo parametros = null;

            if (mListaConfiguracionEnvioCorreo.ContainsKey(pProyectoID))
            {
                parametros = mListaConfiguracionEnvioCorreo[pProyectoID];
            }
            else
            {
                ParametroCN parametroCN = new ParametroCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<ParametroCN>(), mLoggerFactory);
                parametros = parametroCN.ObtenerConfiguracionEnvioCorreo(pProyectoID);
                parametroCN.Dispose();
                mListaConfiguracionEnvioCorreo.Add(pProyectoID, parametros);
            }

            if (parametros == null && pProyectoID != ProyectoAD.MetaProyecto)
            {
                parametros = ObtenerParametrosConfiguracionEnvioCorreo(ProyectoAD.MetaProyecto, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
            }

            return parametros;
        }

        /// <summary>
        /// Procesa las newsletter pendientes de envío para encolarlas en ColaCorreo
        /// </summary>
        /// <returns>Estado del resultado de la operacion sobre las newsletter</returns>
        private LogStatus EnviarNewsLetter(EntityContext entityContext, LoggingService loggingService, EntityContextBASE entityContextBASE, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication, IAvailableServices availableServices, bool esRabbit = false)
        {
            LogStatus estadoProceso = LogStatus.Correcto;
            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<DocumentacionCN>(), mLoggerFactory);
            mGestionNotificaciones = new GestionNotificaciones(new DataWrapperNotificacion(), loggingService, entityContext, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<GestionNotificaciones>(), mLoggerFactory);

            string cabecera = "";
            string pie = "";

            if (mListaNewsletter.Count > 0)
            {
                BaseComunidadCN baseComCN = new BaseComunidadCN("base", entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<BaseComunidadCN>(), mLoggerFactory);
                List<string> listaCorreosID = new List<string>();
                foreach (DocumentoNewsletter docNews in mListaNewsletter)
                {
                    docNews.EnvioSolicitado = true;

                    try
                    {
                        List<Guid> listaGruposID = null;
                        if (!string.IsNullOrEmpty(docNews.GruposID))
                        {
                            listaGruposID = ObtenerGruposNewsletter(docNews.GruposID);
                        }

                        ObtenerCabeceraYPie(docNews.ProyectoID, docNews.IdiomaDestinatarios, ref cabecera, ref pie, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                        string mascaraFrom = UtilCorreo.MASCARA_GENERICA;

                        if (docNews.ProyectoID != ProyectoAD.MyGnoss && docNews.ProyectoID != Guid.Empty)
                        {
                            ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<ProyectoCN>(), mLoggerFactory);
                            mascaraFrom = proyCN.ObtenerNombreDeProyectoID(docNews.ProyectoID);
                            proyCN.Dispose();
                        }

                        ConfiguracionEnvioCorreo parametrosCorreo = ObtenerParametrosConfiguracionEnvioCorreo(docNews.ProyectoID, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                        //obtener destinatarios
                        List<string> listaEmailsDestinatarios = ObtenerDestinatariosNewsletter(listaGruposID, docNews.ProyectoID, docNews.IdiomaDestinatarios, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                        string cuerpo = string.Empty;
                        bool esHtml = true;

                        if (!string.IsNullOrEmpty(docNews.Descripcion))
                        {
                            cuerpo = cabecera + docNews.Descripcion + pie;
                        }
                        else
                        {
                            cuerpo = docNews.HtmlNewsletter;
                        }

                        if (!string.IsNullOrEmpty(cuerpo))
                        {
                            int correoID = baseComCN.InsertarCorreo(parametrosCorreo, listaEmailsDestinatarios, docNews.Titulo, cuerpo, esHtml, mascaraFrom, availableServices);
                            listaCorreosID.Add(JsonConvert.SerializeObject(correoID));
                        }

                        docNews.EnvioRealizado = true;
                    }
                    catch (Exception ex)
                    {
                        docNews.EnvioRealizado = false;
                        loggingService.GuardarLog(ex.Message, mlogger);
                    }
                }

                baseComCN.InsertarCorreosIDColaCorreoRabbitMQ(listaCorreosID);
                baseComCN.Dispose();

                if (!esRabbit)
                {
                    foreach (DocumentoNewsletter docNews in mListaNewsletter)
                    {
                        try
                        {
                            docCN.ActuarlizarEnvioRealizadoDocumentoEnvioNewsletter(docNews.EnvioSolicitado, docNews.EnvioRealizado, docNews.DocumentoID, docNews.IdentidadID, docNews.Fecha);
                        }
                        catch (Exception ex)
                        {
                            loggingService.GuardarLog(ex.Message, mlogger);
                        }
                    }
                }

                docCN.Dispose();
            }
            else
            {
                estadoProceso = LogStatus.NoEnviado;
            }

            return estadoProceso;
        }

        private static List<Guid> ObtenerGruposNewsletter(string pGrupos)
        {
            List<Guid> listaGrupos = null;

            if (!string.IsNullOrEmpty(pGrupos))
            {
                string[] array = pGrupos.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                if (array.Length > 0)
                {
                    listaGrupos = new List<Guid>();
                    foreach (string grupo in array)
                    {
                        Guid grupoID;
                        if (Guid.TryParse(grupo, out grupoID))
                        {
                            listaGrupos.Add(grupoID);
                        }
                    }
                }
            }

            return listaGrupos;
        }

        private List<string> ObtenerDestinatariosNewsletter(List<Guid> pListaGruposID, Guid pProyectoID, string pIdioma, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            List<string> listaEmails = new List<string>();
            List<AD.EntityModel.Models.PersonaDS.Persona> listaPersona;
            PersonaCN personaCN = new PersonaCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<PersonaCN>(), mLoggerFactory);
            IdentidadCN identidadCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<IdentidadCN>(), mLoggerFactory);

            if (pListaGruposID != null && pListaGruposID.Count > 0)
            {
                //sólo los miembros del grupo
                listaPersona = personaCN.ObtenerPersonasDeGruposDeProyectoCargaLigera(pListaGruposID);
            }
            else
            {
                //todos los miembros de la comunidad
                listaPersona = personaCN.ObtenerPersonasDeProyectoCargaLigera(ProyectoAD.MetaOrganizacion, pProyectoID);
            }

            DataWrapperIdentidad dataWrapperIdentidad = identidadCN.ObtenerIdentidadesDeProyecto(pProyectoID);

            foreach (AD.EntityModel.Models.PersonaDS.Persona filaPersona in listaPersona)
            {
                List<AD.EntityModel.Models.IdentidadDS.Perfil> filasPerfil = dataWrapperIdentidad.ListaPerfil.Where(perfil => perfil.PersonaID.Equals(filaPersona.PersonaID)).ToList();

                if (filasPerfil.Count > 0)
                {
                    AD.EntityModel.Models.IdentidadDS.Perfil perfilPersona = filasPerfil[0];

                    List<AD.EntityModel.Models.IdentidadDS.Identidad> filasIdentidadPersona = dataWrapperIdentidad.ListaIdentidad.Where(identidad => identidad.PerfilID.Equals(perfilPersona.PerfilID)).ToList();

                    if (filasIdentidadPersona.Count > 0)
                    {
                        AD.EntityModel.Models.IdentidadDS.Identidad identidadPersona = filasIdentidadPersona[0];

                        if (identidadPersona.RecibirNewsLetter && filaPersona.Idioma == pIdioma && filaPersona.Email != null)
                        {
                            listaEmails.Add(filaPersona.Email);
                        }
                    }
                }
            }

            identidadCN.Dispose();
            personaCN.Dispose();
            return listaEmails;
        }

        private void ObtenerCabeceraYPie(Guid pProyectoID, string idioma, ref string cabecera, ref string pie, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            string urlContent = "";
            ParametroAplicacionCN paramCN = new ParametroAplicacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<ParametroAplicacionCN>(), mLoggerFactory);

            urlContent = paramCN.ObtenerUrlContent(pProyectoID);
            paramCN.Dispose();

            cabecera = mGestionNotificaciones.ListaFormatosCorreo(idioma)["cabecera"];
            Proyecto filaProyecto = null;
            GestorParametroGeneral filaParamGeneral = new GestorParametroGeneral();
            if (pProyectoID != ProyectoAD.MyGnoss && pProyectoID != Guid.Empty)
            {
                ParametroGeneralGBD gestorParametroGeneralController = new ParametroGeneralGBD(entityContext);
                filaParamGeneral = gestorParametroGeneralController.ObtenerFilaParametrosGeneralesDeProyecto(filaParamGeneral, pProyectoID);

                ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<ProyectoCN>(), mLoggerFactory);
                filaProyecto = proyCN.ObtenerProyectoPorIDCargaLigera(pProyectoID);
                short tipoProyecto = filaProyecto.TipoProyecto;

                if (tipoProyecto == (short)TipoProyecto.Universidad20)
                {
                    cabecera = cabecera.Replace("IMAGENPROYECTO", $"<img src=\"{urlContent}/img/Miclaseuni20.jpg\">");
                }
                else if (tipoProyecto == (short)TipoProyecto.EducacionExpandida)
                {
                    cabecera = cabecera.Replace("IMAGENPROYECTO", $"<img src=\"{urlContent}/img/miclseeduexpandida.jpg\">");
                }
                else if (tipoProyecto == (short)TipoProyecto.EducacionPrimaria)
                {
                    cabecera = cabecera.Replace("IMAGENPROYECTO", $"<img src=\"{urlContent}/img/miclaseeduprimaria.jpg\">");
                }
                else
                {
                    if (filaParamGeneral.ListaParametroGeneral.FirstOrDefault().CoordenadasSup != "")
                    {
                        cabecera = cabecera.Replace("IMAGENPROYECTO", $"<img src=\"{urlContent}/Imagenes/Proyectos/{pProyectoID.ToString()}.png\">");
                    }
                    else
                    {
                        cabecera = cabecera.Replace("IMAGENPROYECTO", $"<p style=\"font-size:30px;color:#8186BD;\">{filaProyecto.Nombre}</p>");
                    }
                }
            }
            else
            {
                cabecera = cabecera.Replace("IMAGENPROYECTO", $"<img src=\"{urlContent}/img/logoGnoss.gif\">");
            }

            pie = mGestionNotificaciones.ListaFormatosCorreo(idioma)["pieNewsletter"];

            if (pie.Contains("<#URLCOMUNIDAD#>") || pie.Contains("<#NOMBRECOMUNIDAD#>") || pie.Contains("<#POLITICAPRIVACIDAD#>") || pie.Contains("<#CONDICIONESUSO#>") || pie.Contains("<#URCONNOMBRECOMUNIDAD#>") || pie.Contains("<#SECCIONNOTIFICACIONES#>"))
            {
                UtilIdiomas utilIdiomas = new UtilIdiomas(idioma, loggingService, entityContext, mConfigService, null, mLoggerFactory.CreateLogger<UtilIdiomas>(), mLoggerFactory);
                string urlBaseProyecto = UtilCadenas.ObtenerTextoDeIdioma(filaProyecto.URLPropia, idioma, idioma);
                string nombrecortoComunidad = "";
                if (pProyectoID != ProyectoAD.MetaProyecto)
                {
                    nombrecortoComunidad = filaProyecto.NombreCorto;
                }
                string URLCOMUNIDAD = "http://www.gnoss.com";
                string NOMBRECOMUNIDAD = "www.gnoss.com";
                string POLITICAPRIVACIDAD = "http://www.gnoss.com/politica-privacidad";
                string CONDICIONESUSO = "http://www.gnoss.com/condiciones-uso";
                string URCONNOMBRECOMUNIDAD = utilIdiomas.GetText("METABUSCADOR", "TODASCOMUNIDADES");
                string SECCIONNOTIFICACIONES = $"<a href=\"{urlBaseProyecto}/editar-perfil-notificacion\">{utilIdiomas.GetText("SUSCRIPCIONES", "SECCIONNOTIFICACIONPERFIL")}</a>";

                if (filaProyecto != null)
                {
                    URLCOMUNIDAD = filaProyecto.URLPropia;
                    NOMBRECOMUNIDAD = filaProyecto.Nombre;
                    POLITICAPRIVACIDAD = URLCOMUNIDAD;
                    CONDICIONESUSO = URLCOMUNIDAD;

                    if (idioma != "es")
                    {
                        URLCOMUNIDAD = $"{URLCOMUNIDAD}/{idioma}";
                        POLITICAPRIVACIDAD = $"{POLITICAPRIVACIDAD}/{idioma}";
                        CONDICIONESUSO = $"{CONDICIONESUSO}/{idioma}";
                        urlBaseProyecto = $"{urlBaseProyecto}/{idioma}";
                    }

                    URLCOMUNIDAD = $"{URLCOMUNIDAD}/{utilIdiomas.GetText("URLSEM", "COMUNIDAD")}/{filaProyecto.NombreCorto}";
                    POLITICAPRIVACIDAD = $"{POLITICAPRIVACIDAD}/{utilIdiomas.GetText("URLSEM", "COMUNIDAD")}/{filaProyecto.NombreCorto}/{utilIdiomas.GetText("URLSEM", "POLITICAPRIVACIDAD")}";
                    CONDICIONESUSO = $"{CONDICIONESUSO}/{utilIdiomas.GetText("URLSEM", "COMUNIDAD")}/{filaProyecto.NombreCorto}/{utilIdiomas.GetText("URLSEM", "CONDICIONESUSO")}";
                }

                if (!string.IsNullOrEmpty(nombrecortoComunidad))
                {
                    string nombreProyecto = UtilCadenas.ObtenerTextoDeIdioma(filaProyecto.Nombre, idioma, idioma);
                    URCONNOMBRECOMUNIDAD = $"<a href=\"{urlBaseProyecto}\">{nombreProyecto}</a>";
                    SECCIONNOTIFICACIONES = $"<a href=\"{urlBaseProyecto}/{utilIdiomas.GetText("URLSEM", "ADMINISTRARSUSCRIPCIONCOMUNIDAD")}\">{utilIdiomas.GetText("SUSCRIPCIONES", "SECCIONSUSCRIBETECOMUNIDAD", nombreProyecto)}</a>";
                }

                pie = pie.Replace("<#NOMBRECOMUNIDAD#>", NOMBRECOMUNIDAD);
                pie = pie.Replace("<#URLCOMUNIDAD#>", URLCOMUNIDAD);
                pie = pie.Replace("<#POLITICAPRIVACIDAD#>", POLITICAPRIVACIDAD);
                pie = pie.Replace("<#CONDICIONESUSO#>", CONDICIONESUSO);
                pie = pie.Replace("<#URCONNOMBRECOMUNIDAD#>", URCONNOMBRECOMUNIDAD);
                pie = pie.Replace("<#SECCIONNOTIFICACIONES#>", SECCIONNOTIFICACIONES);
                
            }
        }

        /// <summary>
        /// Genera el texto de la entrada del log para un envío de notificaciones
        /// </summary>
        /// <param name="pStatus">Estado de la operación de envio</param>
        /// <param name="pDetalles">Detalles de la operación de envio</param>
        /// <returns>Texto incluyendo estado y detalles del envío</returns>
        private static string CrearEntradaRegistro(LogStatus pEstado, string pDetalles)
        {
            string entradaLog = string.Empty;

            switch (pEstado)
            {
                case LogStatus.Correcto:
                    entradaLog = "\r\n\t >> OK: ";
                    break;
                case LogStatus.Error:
                    entradaLog = "\r\n\t >> ALERT: ";
                    break;
                case LogStatus.NoEnviado:
                    entradaLog = "\r\n\t >> OK: ";
                    break;
            }
            return entradaLog + pDetalles;
        }

        /// <summary>
        /// Carga las notificaciones que deben enviarse
        /// </summary>
        private void CargarDatosRabbit(AD.EntityModel.Models.Documentacion.DocumentoEnvioNewsLetter pDocumentoEnvioNewsLetter, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<DocumentacionCN>(), mLoggerFactory);
            List<AD.EntityModel.Models.Documentacion.NewsletterPendientes> listaNewsletterPendientes = docCN.ObtenerDocumentoEnvioNewsletterPendienteEnvioRabbit(pDocumentoEnvioNewsLetter);
            mListaNewsletter = new List<DocumentoNewsletter>();
            foreach (AD.EntityModel.Models.Documentacion.NewsletterPendientes fila in listaNewsletterPendientes)
            {
                try
                {
                    DocumentoNewsletter docNews = new DocumentoNewsletter();
                    docNews.DocumentoID = fila.DocumentoID;
                    docNews.ProyectoID = fila.ProyectoID.Value;
                    docNews.Titulo = fila.Titulo;
                    docNews.Descripcion = fila.Descripcion;
                    docNews.IdentidadID = fila.IdentidadID;
                    docNews.IdiomaDestinatarios = fila.Idioma;

                    bool solicitado = false;
                    bool.TryParse(fila.EnvioSolicitado.ToString(), out solicitado);
                    docNews.EnvioSolicitado = solicitado;

                    bool enviado = false;
                    bool.TryParse(fila.EnvioRealizado.ToString(), out enviado);
                    docNews.EnvioRealizado = enviado;
                    docNews.GruposID = fila.Grupos;
                    docNews.HtmlNewsletter = fila.Newsletter;
                    DateTime fecha = fila.Fecha;
                    docNews.Fecha = fecha;
                    mListaNewsletter.Add(docNews);
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLog(ex.Message, mlogger);
                }
            }
            docCN.Dispose();
        }

        /// <summary>
        /// Carga las notificaciones que deben enviarse
        /// </summary>
        private void CargarDatos(EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication, Guid? pDocumentoID = null)
        {
            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<DocumentacionCN>(), mLoggerFactory);
            List<AD.EntityModel.Models.Documentacion.NewsletterPendientes> listaNewsletterPendientes = docCN.ObtenerDocumentoEnvioNewsletterPendienteEnvio(pDocumentoID);
            mListaNewsletter = new List<DocumentoNewsletter>();
            foreach (AD.EntityModel.Models.Documentacion.NewsletterPendientes fila in listaNewsletterPendientes)
            {
                try
                {
                    DocumentoNewsletter docNews = new DocumentoNewsletter();
                    docNews.DocumentoID = fila.DocumentoID;
                    docNews.ProyectoID = fila.ProyectoID.Value;
                    docNews.Titulo = fila.Titulo;
                    docNews.Descripcion = fila.Descripcion;
                    docNews.IdentidadID = fila.IdentidadID;
                    docNews.IdiomaDestinatarios = fila.Idioma;

                    bool solicitado = false;
                    bool.TryParse(fila.EnvioSolicitado.ToString(), out solicitado);
                    docNews.EnvioSolicitado = solicitado;

                    bool enviado = false;
                    bool.TryParse(fila.EnvioRealizado.ToString(), out enviado);
                    docNews.EnvioRealizado = enviado;
                    docNews.GruposID = fila.Grupos;
                    docNews.HtmlNewsletter = fila.Newsletter;
                    DateTime fecha = fila.Fecha;
                    docNews.Fecha = fecha;
                    mListaNewsletter.Add(docNews);
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLog(ex.Message, mlogger);
                }
            }
            docCN.Dispose();
        }

        #endregion

        #endregion
    }
}
