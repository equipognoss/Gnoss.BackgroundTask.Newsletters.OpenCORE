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

namespace Es.Riam.Gnoss.Win.ServicioEnviosMasivos
{
    public class Controller : ControladorServicioGnoss
    {
        #region Constantes

        private string COLA_NEWSLETTER = "ColaNewsletter";
        private string EXCHANGE = "";

        #endregion

        #region Estaticos

        private static object OBJETO_BLOQUEO_ENVIO_NEWSLETTER = new object();

        #endregion

        #region Miembros

        /// <summary>
        /// Guarda una relación de las notificaciones enviadas y su fecha de envío. Sólo contendrá notificaciones enviadas recientemente (1 día).
        /// </summary>
        private Dictionary<Guid, DateTime> mNotificacionesYaEnviadas;

        /// <summary>
        /// Gestor de notificaciones
        /// </summary>
        private GestionNotificaciones mGestionNotificaciones;

        /// <summary>
        /// Lista de DocumentoNewsletter
        /// </summary>
        private List<DocumentoNewsletter> mListaNewsletter;

        public Dictionary<Guid, ConfiguracionEnvioCorreo> mListaConfiguracionEnvioCorreo;        

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor a partir de la base de datos pasada por parámetro
        /// </summary>
        /// <param name="pBaseDeDatos">Base de datos</param>
        public Controller(IServiceScopeFactory scopedFactory, ConfigService configService)
            : base(scopedFactory, configService)
        {
            mListaConfiguracionEnvioCorreo = new Dictionary<Guid, ConfiguracionEnvioCorreo>();
        }

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new Controller(ScopedFactory, mConfigService);
        }

        #endregion

        #region Métodos

        #region Públicos

       

        /// <summary>
        /// Realiza el envio de las notificaciones pendientes de enviar 
        /// y escribe en el log una entrada indicando el resultado de la operación
        /// </summary>
        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            mNotificacionesYaEnviadas = new Dictionary<Guid, DateTime>();


            RealizarMantenimientoRabbitMQ(loggingService);
            RealizarMantenimientoBD();
        }

        #endregion

        #region Privados

        private void RealizarMantenimientoBD()
        {
            LogStatus estadoProceso = LogStatus.Error;
            String entradaLog = String.Empty;

            while (true)
            {
                using (var scope = ScopedFactory.CreateScope())
                {
                    EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                    EntityContextBASE entityContextBASE = scope.ServiceProvider.GetRequiredService<EntityContextBASE>();
                    LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                    IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
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
                            //estadoProceso = EnviarNotificaciones();

                            estadoProceso = EnviarNewsLetter(entityContext, loggingService, entityContextBASE, servicesUtilVirtuosoAndReplication);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch (Exception ex)
                        {
                            loggingService.GuardarLog(LogStatus.Error.ToString().ToUpper() + " (" + mFicheroConfiguracionBD + ") " + CrearEntradaRegistro(LogStatus.Error, ex.Message));
                        }
                        finally
                        {
                            //Dormimos el proceso el tiempo establecido
                            Thread.Sleep(Controller.INTERVALO_SEGUNDOS * 1000);
                        }
                    }
                }
            }
        }

        private void RealizarMantenimientoRabbitMQ(LoggingService loggingService, bool reintentar = true)
        {

            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItem);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);

                RabbitMQClient rabbitMQClient = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_NEWSLETTER, loggingService, mConfigService, EXCHANGE, COLA_NEWSLETTER);

                try
                {
                    rabbitMQClient.ObtenerElementosDeCola(funcionProcesarItem, funcionShutDown);
                    mReiniciarLecturaRabbit = false;
                }
                catch (Exception ex)
                {
                    mReiniciarLecturaRabbit = true;
                    loggingService.GuardarLogError(ex);
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
                VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                try
                {
                    ComprobarCancelacionHilo();

                    Debug.WriteLine($"ProcesarItem, {pFila}!");

                    if (!string.IsNullOrEmpty(pFila))
                    {

                        AD.EntityModel.Models.Documentacion.DocumentoEnvioNewsLetter documentoEnvioNewsLetter = JsonConvert.DeserializeObject<AD.EntityModel.Models.Documentacion.DocumentoEnvioNewsLetter>(pFila);

                        ProcesarFilaDeCola(documentoEnvioNewsLetter, loggingService, entityContext, entityContextBASE, servicesUtilVirtuosoAndReplication);

                        documentoEnvioNewsLetter = null;

                        servicesUtilVirtuosoAndReplication.ConexionAfinidad = "";

                        ControladorConexiones.CerrarConexiones(false);

                    }

                    return true;
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLogError(ex);
                    return true;
                }
            }
        }

        private void ProcesarFilaDeCola(AD.EntityModel.Models.Documentacion.DocumentoEnvioNewsLetter pDocumentoEnvioNewsLetter, LoggingService loggingService, EntityContext entityContext, EntityContextBASE entityContextBASE, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            lock (OBJETO_BLOQUEO_ENVIO_NEWSLETTER)
            {
                LogStatus estadoProceso = LogStatus.Error;
                string entradaLog = string.Empty;

                try
                {
                    ComprobarCancelacionHilo();
                    //(Re)Carga los datos de la BD referentes a notificaciones
                    CargarDatosRabbit(pDocumentoEnvioNewsLetter, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                    estadoProceso = EnviarNewsLetter(entityContext, loggingService, entityContextBASE, servicesUtilVirtuosoAndReplication, true);
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLog(LogStatus.Error.ToString().ToUpper() + " (" + mFicheroConfiguracionBD + ") " + CrearEntradaRegistro(LogStatus.Error, ex.Message));
                }
            }
        }

        private UtilCorreo ObtenerGestorCorreoDeProyecto(Guid pProyectoID, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ConfiguracionEnvioCorreo parametros = ObtenerParametrosConfiguracionEnvioCorreo(pProyectoID, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

            if (parametros == null)
            {
                throw new Exception("No Existe la configuración de envio de correo en la Base De Datos, Consulta la tabla 'ConfiguracionEnvioCorreo'");
            }

            return new UtilCorreo(parametros.smtp, parametros.puerto, parametros.usuario, parametros.clave, parametros.SSL);
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
                ParametroCN parametroCN = new ParametroCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
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
        /// Realiza el envío de las notificaciones
        /// </summary>
        /// <returns>Estado del resultado de la operacion del envio de las notificaciones</returns>
        private LogStatus EnviarNotificaciones(EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            LogStatus estadoProceso = LogStatus.Correcto;
            mGestionNotificaciones.CargarGestor();
            //guardar un diccionario de ids enviadas y con idnotificacion y fecha proceso, limpiar la lista dia (cargar las que lleven menos de un dia)

            if (mGestionNotificaciones.ListaNotificaciones.Values.Count > 0)
            {
                string cabecera = "";
                string pie = "";

                //string urlIntraGNOSS = "";
                string urlContent = "";
                ParametroAplicacionCN paramCN = new ParametroAplicacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                //ParametroAplicacionDS paramDS = paramCN.ObtenerConfiguracionGnoss();

                //ParametroAplicacionDS gestorParametroAplicacion = paramCN.ObtenerConfiguracionBBDD();
                ParametroAplicacionGBD paramatroAPlicacionGBD = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
                GestorParametroAplicacion gestorParametroAplicacion = new GestorParametroAplicacion();
                paramatroAPlicacionGBD.ObtenerConfiguracionBBDD(gestorParametroAplicacion);
                //urlIntraGNOSS = ((ParametroAplicacionDS.ParametroAplicacionRow[])(paramDS.ParametroAplicacion.Select("parametro='UrlIntragnoss'")))[0].Valor;
                //urlContent = gestorParametroAplicacion.ConfiguracionServicios.Select("nombre='urlContent'")[0].Url;
                urlContent = gestorParametroAplicacion.ListaConfiguracionServicios.FirstOrDefault(configuracionServicios=> configuracionServicios.Nombre.Equals("urlContent")).Url;
                //paramCN.Dispose();

                foreach (Notificacion notificacion in mGestionNotificaciones.ListaNotificaciones.Values)
                {
                    string mascaraFrom = UtilCorreo.MASCARA_GENERICA;

                    if (notificacion.FilaNotificacion.ProyectoID.HasValue && notificacion.FilaNotificacion.ProyectoID != ProyectoAD.MyGnoss && notificacion.FilaNotificacion.ProyectoID != Guid.Empty)
                    {
                        ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                        mascaraFrom = proyCN.ObtenerNombreDeProyectoID(notificacion.FilaNotificacion.ProyectoID.Value);
                        proyCN.Dispose();
                    }

                    AD.EntityModel.Models.Notificacion.NotificacionEnvioMasivo enviosMasivo = notificacion.FilaNotificacion.NotificacionEnvioMasivo;

                    if (enviosMasivo != null)
                    {
                        EstadoEnvio estadoEnvio = EstadoEnvio.Error;

                        //se limpia el diccionario de notificaciones masivas cuya fecha de envío sea superior a 24h
                        ActualizarNotificacionesYaEnviadas();

                        if (!mNotificacionesYaEnviadas.ContainsKey(enviosMasivo.NotificacionID))
                        {
                            try
                            {
                                //la fila está como pendiente y no ha sido ya enviada recientemente
                                if (enviosMasivo.EstadoEnvio == (short)EstadoEnvio.Pendiente)
                                {
                                    //Obtenemos el mensaje y el asunto en el idioma de la notificacion, 
                                    //y si no en castellano
                                    string idioma = "";

                                    if (!enviosMasivo.Notificacion.Idioma.Equals(null) && enviosMasivo.Notificacion.Idioma.Trim() != "")
                                    {
                                        idioma = enviosMasivo.Notificacion.Idioma;
                                    }
                                    else
                                    {
                                        idioma = "es";
                                    }

                                    cabecera = mGestionNotificaciones.ListaFormatosCorreo(idioma)["cabecera"];

                                    Proyecto filaProyecto = null;
                                    GestorParametroGeneral filaParamGeneral = new GestorParametroGeneral();

                                    if (notificacion.FilaNotificacion.ProyectoID.HasValue && notificacion.FilaNotificacion.ProyectoID != ProyectoAD.MyGnoss && notificacion.FilaNotificacion.ProyectoID != Guid.Empty)
                                    {
                                        ParametroGeneralGBD gestorParametroGeneral=new ParametroGeneralGBD(entityContext);

                                        //ParametroGeneralCN paramGeneralCN = new ParametroGeneralCN(mFicheroConfiguracionBD);
                                        //filaParamGeneral = paramGeneralCN.ObtenerFilaParametrosGeneralesDeProyecto(notificacion.FilaNotificacion.ProyectoID);
                                        filaParamGeneral = gestorParametroGeneral.ObtenerFilaParametrosGeneralesDeProyecto(filaParamGeneral, notificacion.FilaNotificacion.ProyectoID.Value);

                                        ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                                        filaProyecto = proyCN.ObtenerProyectoPorIDCargaLigera(notificacion.FilaNotificacion.ProyectoID.Value);
                                        short tipoProyecto = filaProyecto.TipoProyecto;

                                        if (tipoProyecto == (short)TipoProyecto.Universidad20)
                                        {
                                            cabecera = cabecera.Replace("IMAGENPROYECTO", "<img src=\"" + urlContent + "/img/Miclaseuni20.jpg\">");
                                        }
                                        else if (tipoProyecto == (short)TipoProyecto.EducacionExpandida)
                                        {
                                            cabecera = cabecera.Replace("IMAGENPROYECTO", "<img src=\"" + urlContent + "/img/miclseeduexpandida.jpg\">");
                                        }
                                        else if (tipoProyecto == (short)TipoProyecto.EducacionPrimaria)
                                        {
                                            cabecera = cabecera.Replace("IMAGENPROYECTO", "<img src=\"" + urlContent + "/img/miclaseeduprimaria.jpg\">");
                                        }
                                        else
                                        {

                                            if (filaParamGeneral.ListaParametroGeneral.FirstOrDefault().CoordenadasSup != "")
                                            {
                                                cabecera = cabecera.Replace("IMAGENPROYECTO", "<img src=\"" + urlContent + "/Imagenes/Proyectos/" + notificacion.FilaNotificacion.ProyectoID.ToString() + ".png\">");
                                            }
                                            else
                                            {
                                                cabecera = cabecera.Replace("IMAGENPROYECTO", "<p style=\"font-size:30px;color:#8186BD;\">" + filaProyecto.Nombre + "</p>");
                                            }
                                        }
                                    }
                                    else
                                    {
                                        cabecera = cabecera.Replace("IMAGENPROYECTO", "<img src=\"" + urlContent + "/img/logoGnoss.gif\">");
                                    }

                                    pie = mGestionNotificaciones.ListaFormatosCorreo(idioma)["pieNewsletter"];


                                    if (pie.Contains("<#URLCOMUNIDAD#>") || pie.Contains("<#NOMBRECOMUNIDAD#>") || pie.Contains("<#POLITICAPRIVACIDAD#>") || pie.Contains("<#CONDICIONESUSO#>"))
                                    {
                                        GestorParametroGeneral paramGeneralDS = null;
                                        if (filaParamGeneral != null)
                                        {
                                            paramGeneralDS = filaParamGeneral;
                                        }

                                        UtilIdiomas utilIdiomas = new UtilIdiomas(idioma, loggingService, entityContext, mConfigService);
                                        string URLCOMUNIDAD = "http://www.gnoss.com";
                                        string NOMBRECOMUNIDAD = "www.gnoss.com";
                                        string POLITICAPRIVACIDAD = "http://www.gnoss.com/politica-privacidad";
                                        string CONDICIONESUSO = "http://www.gnoss.com/condiciones-uso";


                                        if (filaProyecto != null)
                                        {
                                            URLCOMUNIDAD = filaProyecto.URLPropia;
                                            NOMBRECOMUNIDAD = filaProyecto.Nombre;
                                            POLITICAPRIVACIDAD = URLCOMUNIDAD;
                                            CONDICIONESUSO = URLCOMUNIDAD;

                                            if (idioma != "es")
                                            {
                                                URLCOMUNIDAD += "/" + idioma;
                                                POLITICAPRIVACIDAD += "/" + idioma;
                                                CONDICIONESUSO += "/" + idioma;
                                            }

                                            URLCOMUNIDAD += "/" + utilIdiomas.GetText("URLSEM", "COMUNIDAD") + "/" + filaProyecto.NombreCorto;
                                            POLITICAPRIVACIDAD += "/" + utilIdiomas.GetText("URLSEM", "COMUNIDAD") + "/" + filaProyecto.NombreCorto + "/" + utilIdiomas.GetText("URLSEM", "POLITICAPRIVACIDAD");
                                            CONDICIONESUSO += "/" + utilIdiomas.GetText("URLSEM", "COMUNIDAD") + "/" + filaProyecto.NombreCorto + "/" + utilIdiomas.GetText("URLSEM", "CONDICIONESUSO");
                                        }

                                        pie = pie.Replace("<#NOMBRECOMUNIDAD#>", NOMBRECOMUNIDAD);
                                        pie = pie.Replace("<#URLCOMUNIDAD#>", URLCOMUNIDAD);
                                        pie = pie.Replace("<#POLITICAPRIVACIDAD#>", POLITICAPRIVACIDAD);
                                        pie = pie.Replace("<#CONDICIONESUSO#>", CONDICIONESUSO);
                                    }

                                    string miAsunto = mGestionNotificaciones.ListaMensajes(idioma)[enviosMasivo.Notificacion.MensajeID].Key;
                                    string miMensaje = mGestionNotificaciones.ListaMensajes(idioma)[enviosMasivo.Notificacion.MensajeID].Value;

                                    Dictionary<string, string> listaParametros = new Dictionary<string, string>();

                                    foreach (AD.EntityModel.Models.Notificacion.NotificacionParametro filaParametro in notificacion.FilaNotificacion.NotificacionParametro.ToList())
                                    {
                                        string nombreParametro = GestionNotificaciones.TextoDeParametro(filaParametro.ParametroID);
                                        string valor = filaParametro.Valor;

                                        listaParametros.Add(nombreParametro, valor);
                                    }

                                    bool esNewsletterCompleta = false;

                                    foreach (string parametro in listaParametros.Keys)
                                    {
                                        if (parametro != GestionNotificaciones.TextoDeParametro((short)ClavesParametro.LogoGnoss))
                                        {
                                            miMensaje = miMensaje.Replace(parametro, listaParametros[parametro]);
                                            miAsunto = miAsunto.Replace(parametro, listaParametros[parametro]);
                                            pie = pie.Replace(parametro, listaParametros[parametro]);
                                        }

                                        if (parametro == GestionNotificaciones.TextoDeParametro((short)ClavesParametro.NewsletterCompleta))
                                        {
                                            esNewsletterCompleta = true;
                                        }
                                    }
                                    //Obtenemos la lista de correos
                                    string email = enviosMasivo.Destinatarios;

                                    if (!string.IsNullOrEmpty(email))
                                    {
                                        //Quitamos los correos incorrectos
                                        email = QuitarEmailsIncorrectos(email);

                                        Guid proyectoID = ProyectoAD.MetaProyecto;
                                        if (filaProyecto != null)
                                        {
                                            proyectoID = filaProyecto.ProyectoID;
                                        }

                                        UtilCorreo gestorCorreo = ObtenerGestorCorreoDeProyecto(proyectoID, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                                        string emailEnvio = ObtenerParametrosConfiguracionEnvioCorreo(proyectoID, entityContext, loggingService, servicesUtilVirtuosoAndReplication).email;
                                        //Enviamos la notificación
                                        if (!esNewsletterCompleta)
                                        {
                                            gestorCorreo.EnviarCorreo(email, emailEnvio, mascaraFrom, miAsunto, cabecera + miMensaje + pie, true, enviosMasivo.NotificacionID);
                                        }
                                        else
                                        {
                                            gestorCorreo.EnviarCorreo(email, emailEnvio, mascaraFrom, miAsunto, miMensaje, true, enviosMasivo.NotificacionID);
                                        }
                                    }
                                    estadoEnvio = EstadoEnvio.Enviado;
                                }
                            }
                            catch (SmtpException smtpEx)
                            {
                                //La configuración del buzón de correo (cuenta, usuario, password) es errónea
                                if (smtpEx is SmtpFailedRecipientException)
                                {
                                    string emailIncorrecto = ((SmtpFailedRecipientException)smtpEx).FailedRecipient.Replace(">", "").Remove(0, 1);

                                    if (mGestionNotificaciones.NotificacionDW.ListaEmailIncorrecto.Where(item => item.Email.Equals(emailIncorrecto)).Count() == 0)
                                    {
                                        AD.EntityModel.Models.Notificacion.EmailIncorrecto filaNuevoEmailIncorrecto = new AD.EntityModel.Models.Notificacion.EmailIncorrecto();
                                        filaNuevoEmailIncorrecto.Email = emailIncorrecto;

                                        mGestionNotificaciones.NotificacionDW.ListaEmailIncorrecto.Add(filaNuevoEmailIncorrecto);
                                    }
                                    //Actualiza la fila NotificacionEnvioMasivo con el estado de Pendiente para que sea procesada de nuevo
                                    //notificacionEnvioMasivo.EstadoEnvio = (short)EstadoEnvio.Pendiente;
                                    //estadoProceso = LogStatus.Correcto;

                                    /************************************/

                                    //Aunque se encuentren correos con un dominio/estructura incorrecta, el email llega a todos los usuarios, por lo que marcar el correo como pendiente solo provoca que se mande el correo 1 vez por cada dominio incorrecto que encuentre.

                                    /************************************/

                                    //Actualizamos la fila de NotificacionEnvioMasivo con la fecha actual y el estado de enviado
                                    //GeneralAD generalAD = new GeneralAD(mFicheroConfiguracionBD, false);
                                    //notificacionEnvioMasivo.FechaEnvio = generalAD.HoraServidor;
                                    //notificacionEnvioMasivo.EstadoEnvio = (short)EstadoEnvio.Enviado;
                                    //generalAD.Dispose();
                                    estadoEnvio = EstadoEnvio.Enviado;
                                }
                                else
                                {
                                    //Actualiza la fila NotificacionEnvioMasivo con el estado de error
                                    //notificacionEnvioMasivo.EstadoEnvio = (short)EstadoEnvio.Error;
                                    estadoProceso = LogStatus.Error;
                                    estadoEnvio = EstadoEnvio.Error;

                                    throw new Exception("La configuración del buzón (dirección de correo, usuario y contraseña) no es válida: " + smtpEx.Message + " : " + smtpEx.StackTrace);
                                }
                            }
                            catch (Exception ex)
                            {
                                //Actualiza la fila NotificacionEnvioMasivo con el estado de error
                                //notificacionEnvioMasivo.EstadoEnvio = (short)EstadoEnvio.Error;
                                estadoEnvio = EstadoEnvio.Error;
                                estadoProceso = LogStatus.Error;
                                loggingService.GuardarLog(LogStatus.Error.ToString().ToUpper() + " (" + mFicheroConfiguracionBD + ") " + this.CrearEntradaRegistro(LogStatus.Error, ex.Message));
                            }
                            finally
                            {
                                //Actualizamos la fila de NotificacionEnvioMasivo con la fecha actual y el estado de enviado
                                enviosMasivo.FechaEnvio = DateTime.Now;
                                enviosMasivo.EstadoEnvio = (short)estadoEnvio;

                                //listaEnviadas.add
                                mNotificacionesYaEnviadas.Add(enviosMasivo.NotificacionID, enviosMasivo.FechaEnvio.Value);

                                //Actualizar el estado de notificacionEnvioMasivo con los datos recién recibidos
                                NotificacionCN notCN = new NotificacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                                notCN.ActualizarNotificacion(enviosMasivo.NotificacionID, enviosMasivo.EstadoEnvio, enviosMasivo.FechaEnvio.Value);
                                notCN.Dispose();
                                

                                // TODO: Agregar el número de correos que hay en la newsletter como un campo nuevo en la tabla NotificacionEnvioMasivo.
                                int numEmailsEnvioMasivo = enviosMasivo.Destinatarios.Split('@').Length - 1;
                                //Dormimos 3 segundos por cada email que se ha enviado.
                                int milisegundosDormir = numEmailsEnvioMasivo * 3 * 1000;
                                Thread.Sleep(milisegundosDormir);
                            }
                        }
                        else
                        {
                            loggingService.GuardarLog("La notificación masiva " + enviosMasivo.NotificacionID + " ya ha sido enviada en las últimas 24h.");
                        }
                    }

                }
            }
            else
            {
                estadoProceso = LogStatus.NoEnviado;
            }
            return estadoProceso;
        }

        /// <summary>
        /// Procesa las newsletter pendientes de envío para encolarlas en ColaCorreo
        /// </summary>
        /// <returns>Estado del resultado de la operacion sobre las newsletter</returns>
        private LogStatus EnviarNewsLetter(EntityContext entityContext, LoggingService loggingService, EntityContextBASE entityContextBASE, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication, bool esRabbit = false)
        {
            LogStatus estadoProceso = LogStatus.Correcto;
            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            mGestionNotificaciones = new GestionNotificaciones(new DataWrapperNotificacion(), loggingService, entityContext, mConfigService, servicesUtilVirtuosoAndReplication);

            string cabecera = "";
            string pie = "";

            if (mListaNewsletter.Count > 0)
            {
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

                        //UtilCorreo gestorCorreo = ObtenerGestorCorreoDeProyecto(docNews.ProyectoID);
                        string mascaraFrom = UtilCorreo.MASCARA_GENERICA;

                        if (docNews.ProyectoID != ProyectoAD.MyGnoss && docNews.ProyectoID != Guid.Empty)
                        {
                            ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
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
                            //newslettercompleta = false cabecera + miMensaje + pie
                            cuerpo = cabecera + docNews.Descripcion + pie;
                        }
                        else
                        {
                            //newslettercompleta = true
                            cuerpo = docNews.HtmlNewsletter;
                        }

                        if (!string.IsNullOrEmpty(cuerpo))
                        {
                            BaseComunidadCN baseComCN = new BaseComunidadCN("base", entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                            baseComCN.InsertarCorreo(parametrosCorreo, listaEmailsDestinatarios, docNews.Titulo, cuerpo, esHtml, mascaraFrom);
                            baseComCN.Dispose();
                        }

                        docNews.EnvioRealizado = true;
                    }
                    catch (Exception ex)
                    {
                        docNews.EnvioRealizado = false;
                        loggingService.GuardarLog(ex.Message);
                    }
                }

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
                            loggingService.GuardarLog(ex.Message);
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

        private List<Guid> ObtenerGruposNewsletter(string pGrupos)
        {
            List<Guid> listaGrupos = null;

            if (!string.IsNullOrEmpty(pGrupos))
            {
                string[] array = pGrupos.Split(new char[]{','}, StringSplitOptions.RemoveEmptyEntries);
                if (array.Length > 0)
                {
                    listaGrupos = new List<Guid>();
                    foreach (string grupo in array)
                    {
                        Guid grupoID = Guid.Empty;
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
            PersonaCN personaCN = new PersonaCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            IdentidadCN identidadCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            
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
                    AD.EntityModel.Models.IdentidadDS.Perfil perfilPersona = filasPerfil.First();
                    
                    List<AD.EntityModel.Models.IdentidadDS.Identidad> filasIdentidadPersona = dataWrapperIdentidad.ListaIdentidad.Where(identidad => identidad.PerfilID.Equals(perfilPersona.PerfilID)).ToList();

                    if (filasIdentidadPersona.Count > 0)
                    {
                        AD.EntityModel.Models.IdentidadDS.Identidad identidadPersona = filasIdentidadPersona[0];

                        if (identidadPersona.RecibirNewsLetter)
                        {
                            if (filaPersona.Idioma == pIdioma && filaPersona.Email != null)
                            {
                                listaEmails.Add(filaPersona.Email);
                            }
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
            ParametroAplicacionCN paramCN = new ParametroAplicacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            //ParametroAplicacionDS paramDS = paramCN.ObtenerConfiguracionBBDD();
            //urlContent = ((ParametroAplicacionDS.ConfiguracionServiciosRow[])(paramDS.ConfiguracionServicios.Select("nombre='urlContent'")))[0].Url;
            urlContent = paramCN.ObtenerUrlContent(pProyectoID);
            paramCN.Dispose();

            cabecera = mGestionNotificaciones.ListaFormatosCorreo(idioma)["cabecera"];
            Proyecto filaProyecto = null;
            GestorParametroGeneral filaParamGeneral = new GestorParametroGeneral();
            if (pProyectoID != ProyectoAD.MyGnoss && pProyectoID != Guid.Empty)
            {
                //ParametroGeneralCN paramGeneralCN = new ParametroGeneralCN(mFicheroConfiguracionBD);
                ParametroGeneralGBD gestorParametroGeneralController = new ParametroGeneralGBD(entityContext);
                filaParamGeneral = gestorParametroGeneralController.ObtenerFilaParametrosGeneralesDeProyecto(filaParamGeneral, pProyectoID);
                //filaParamGeneral = paramGeneralCN.ObtenerFilaParametrosGeneralesDeProyecto(pProyectoID);

                ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                filaProyecto = proyCN.ObtenerProyectoPorIDCargaLigera(pProyectoID);
                short tipoProyecto = filaProyecto.TipoProyecto;

                if (tipoProyecto == (short)TipoProyecto.Universidad20)
                {
                    cabecera = cabecera.Replace("IMAGENPROYECTO", "<img src=\"" + urlContent + "/img/Miclaseuni20.jpg\">");
                }
                else if (tipoProyecto == (short)TipoProyecto.EducacionExpandida)
                {
                    cabecera = cabecera.Replace("IMAGENPROYECTO", "<img src=\"" + urlContent + "/img/miclseeduexpandida.jpg\">");
                }
                else if (tipoProyecto == (short)TipoProyecto.EducacionPrimaria)
                {
                    cabecera = cabecera.Replace("IMAGENPROYECTO", "<img src=\"" + urlContent + "/img/miclaseeduprimaria.jpg\">");
                }
                else
                {

                    if (filaParamGeneral.ListaParametroGeneral.FirstOrDefault().CoordenadasSup != "")
                    {
                        cabecera = cabecera.Replace("IMAGENPROYECTO", "<img src=\"" + urlContent + "/Imagenes/Proyectos/" + pProyectoID.ToString() + ".png\">");
                    }
                    else
                    {
                        cabecera = cabecera.Replace("IMAGENPROYECTO", "<p style=\"font-size:30px;color:#8186BD;\">" + filaProyecto.Nombre + "</p>");
                    }
                }
            }
            else
            {
                cabecera = cabecera.Replace("IMAGENPROYECTO", "<img src=\"" + urlContent + "/img/logoGnoss.gif\">");
            }

            pie = mGestionNotificaciones.ListaFormatosCorreo(idioma)["pieNewsletter"];


            if (pie.Contains("<#URLCOMUNIDAD#>") || pie.Contains("<#NOMBRECOMUNIDAD#>") || pie.Contains("<#POLITICAPRIVACIDAD#>") || pie.Contains("<#CONDICIONESUSO#>"))
            {
                GestorParametroGeneral paramGeneralDS = null;
                if (filaParamGeneral != null)
                {
                    paramGeneralDS = filaParamGeneral;
                }

                UtilIdiomas utilIdiomas = new UtilIdiomas(idioma, loggingService, entityContext, mConfigService);
                string URLCOMUNIDAD = "http://www.gnoss.com";
                string NOMBRECOMUNIDAD = "www.gnoss.com";
                string POLITICAPRIVACIDAD = "http://www.gnoss.com/politica-privacidad";
                string CONDICIONESUSO = "http://www.gnoss.com/condiciones-uso";


                if (filaProyecto != null)
                {
                    URLCOMUNIDAD = filaProyecto.URLPropia;
                    NOMBRECOMUNIDAD = filaProyecto.Nombre;
                    POLITICAPRIVACIDAD = URLCOMUNIDAD;
                    CONDICIONESUSO = URLCOMUNIDAD;

                    if (idioma != "es")
                    {
                        URLCOMUNIDAD += "/" + idioma;
                        POLITICAPRIVACIDAD += "/" + idioma;
                        CONDICIONESUSO += "/" + idioma;
                    }

                    URLCOMUNIDAD += "/" + utilIdiomas.GetText("URLSEM", "COMUNIDAD") + "/" + filaProyecto.NombreCorto;
                    POLITICAPRIVACIDAD += "/" + utilIdiomas.GetText("URLSEM", "COMUNIDAD") + "/" + filaProyecto.NombreCorto + "/" + utilIdiomas.GetText("URLSEM", "POLITICAPRIVACIDAD");
                    CONDICIONESUSO += "/" + utilIdiomas.GetText("URLSEM", "COMUNIDAD") + "/" + filaProyecto.NombreCorto + "/" + utilIdiomas.GetText("URLSEM", "CONDICIONESUSO");
                }

                pie = pie.Replace("<#NOMBRECOMUNIDAD#>", NOMBRECOMUNIDAD);
                pie = pie.Replace("<#URLCOMUNIDAD#>", URLCOMUNIDAD);
                pie = pie.Replace("<#POLITICAPRIVACIDAD#>", POLITICAPRIVACIDAD);
                pie = pie.Replace("<#CONDICIONESUSO#>", CONDICIONESUSO);
            }
        }

        /// <summary>
        /// Borra del diccionario las notificaciones masivas que llevan más de 24 horas
        /// </summary>
        private void ActualizarNotificacionesYaEnviadas()
        {
            List<Guid> borradas = new List<Guid>();

            foreach (Guid clave in mNotificacionesYaEnviadas.Keys)
            {
                if (mNotificacionesYaEnviadas[clave] < System.DateTime.Now.AddDays(-1))
                {
                    borradas.Add(clave);
                }
            }
            foreach (Guid clave in borradas)
            {
                mNotificacionesYaEnviadas.Remove(clave);
            }
        }

        /// <summary>
        /// Genera el texto de la entrada del log para un envío de notificaciones
        /// </summary>
        /// <param name="pStatus">Estado de la operación de envio</param>
        /// <param name="pDetalles">Detalles de la operación de envio</param>
        /// <returns>Texto incluyendo estado y detalles del envío</returns>
        private String CrearEntradaRegistro(LogStatus pEstado, String pDetalles)
        {
            String entradaLog = String.Empty;

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
            //NotificacionCN notificacionCN = new NotificacionCN(mFicheroConfiguracionBD, false);
            //notificacionCN = new NotificacionCN(mFicheroConfiguracionBD, false);
            ////Lo metemos dentro del try catch y del while para controlar posibles errores al cargar los datos.
            //mGestionNotificaciones = new GestionNotificaciones(notificacionCN.ObtenerNotificacionesEnviosMasivos());
            //notificacionCN.Dispose();

            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
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
                    loggingService.GuardarLog(ex.Message);
                }
            }
            docCN.Dispose();
        }

        /// <summary>
        /// Carga las notificaciones que deben enviarse
        /// </summary>
        private void CargarDatos(EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication, Guid? pDocumentoID = null)
        {
            //NotificacionCN notificacionCN = new NotificacionCN(mFicheroConfiguracionBD, false);
            //notificacionCN = new NotificacionCN(mFicheroConfiguracionBD, false);
            ////Lo metemos dentro del try catch y del while para controlar posibles errores al cargar los datos.
            //mGestionNotificaciones = new GestionNotificaciones(notificacionCN.ObtenerNotificacionesEnviosMasivos());
            //notificacionCN.Dispose();

            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
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
                catch(Exception ex)
                {
                    loggingService.GuardarLog(ex.Message);
                }
            }
            docCN.Dispose();
        }

        /// <summary>
        /// Elimina las direcciones de correo guardadas como incorrectas de la lista de direcciones pasada como parámetro
        /// </summary>
        /// <param name="pEmail">Cadena de texto con una lista de emails separados por comas</param>
        /// <returns>Lista de emails correctos</returns>
        private string QuitarEmailsIncorrectos(string pEmail)
        {
            string destinatarios = pEmail;

            foreach (AD.EntityModel.Models.Notificacion.EmailIncorrecto filaEmailIncorrecto in mGestionNotificaciones.NotificacionDW.ListaEmailIncorrecto)
            {
                if (destinatarios.Contains(filaEmailIncorrecto.Email))
                {
                    //Quitamos el email incorrecto y un caracter más (la coma separadora)

                    if (destinatarios.Length == destinatarios.IndexOf(filaEmailIncorrecto.Email) + filaEmailIncorrecto.Email.Length)
                    {
                        destinatarios = destinatarios.Remove(destinatarios.IndexOf(filaEmailIncorrecto.Email), filaEmailIncorrecto.Email.Length);
                    }
                    else
                    {
                        destinatarios = destinatarios.Remove(destinatarios.IndexOf(filaEmailIncorrecto.Email), filaEmailIncorrecto.Email.Length + 1);
                    }
                }
            }

            if (destinatarios.Length > 0 && destinatarios.LastIndexOf(",") == destinatarios.Length - 1)
            {
                destinatarios = destinatarios.Substring(0, destinatarios.Length - 1);
            }

            return destinatarios;
        }

        #endregion

        #endregion
    }
}
