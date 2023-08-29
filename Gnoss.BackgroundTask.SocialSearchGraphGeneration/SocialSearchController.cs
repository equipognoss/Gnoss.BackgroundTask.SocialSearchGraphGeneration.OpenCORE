using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Data;
using Es.Riam.Gnoss.AD.BASE_BD.Model;
using Es.Riam.Gnoss.AD.BASE_BD;
using Es.Riam.Gnoss.Logica.BASE_BD;
using Es.Riam.Gnoss.AD.Tags;
using Es.Riam.Gnoss.Logica.ServiciosGenerales;
using Es.Riam.Gnoss.AD.ServiciosGenerales;
using Es.Riam.Gnoss.Logica.Facetado;
using Es.Riam.Gnoss.AD.Facetado;
using Es.Riam.Gnoss.Logica.ParametroAplicacion;
using Es.Riam.Gnoss.Logica.Identidad;
using System.Diagnostics;
using Es.Riam.Gnoss.Recursos;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.Elementos.Identidad;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Util;
using Es.Riam.Gnoss.AD.Facetado.Model;
using Es.Riam.Gnoss.Logica.Live;
using Es.Riam.Gnoss.AD.Organizador.Correo.Model;
using Es.Riam.Gnoss.Logica.Organizador.Correo;
using Es.Riam.Gnoss.AD.Notificacion;
using Es.Riam.Gnoss.Elementos.Notificacion;
using Es.Riam.Gnoss.Elementos.ServiciosGenerales;
using System.Linq;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using Es.Riam.Gnoss.AD.EncapsuladoDatos;
using Es.Riam.Gnoss.RabbitMQ;
using Newtonsoft.Json;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.Gnoss.UtilServiciosWeb;
using Es.Riam.Gnoss.AD.EntityModel.Models.BASE;
using Es.Riam.AbstractsOpen;
using Es.Riam.Gnoss.Logica.Notificacion;

namespace GnossServicioModuloBaseUsuarios
{
    /// <summary>
    /// Controlador que realiza el mantenimiento
    /// </summary>
    internal class SocialSearchController : ControladorServicioGnoss
    {
        #region Constantes

        private const string EXCHANGE = "";
        private const string COLA_TAGS_COMENTARIOS = "ColaTagsComentarios";
        private const string COLA_TAGS_MENSAJES = "ColaTagsMensaje";

        #endregion

        #region Miembros

        private BaseMensajesDS mBaseMensajesDS = null;
        private BaseInvitacionesDS mBaseInvitacionesDS = null;
        private BaseComentariosDS mBaseComentariosDS = null;
        private BaseSuscripcionesDS mBaseSuscripcionesDS = null;
        private BaseContactosDS mBaseContactosDS = null;
        public bool mReplicacion = true;
        private string mUrlServicioEtiquetas = "";
        private bool mReiniciarColaMensajes = false;
        private bool mReiniciarColaComentarios = false;
        private bool mReiniciarColaSuscripciones = false;

        private RabbitMQClient mClienteRabbitMensajes;
        private RabbitMQClient mClienteRabbitComentarios;
        private RabbitMQClient mClienteRabbitSuscripciones;

        #endregion

        #region Constructores

        /// <summary>
        /// Constructor de la clase
        /// </summary>
        /// <param name="pFicheroConfiguracionBD">Fichero de configuración de la base de datos</param>
        /// <param name="pCancellationToken">Token de cancelación para parar el servicio</param>
        /// <param name="pReplicacion">TRUE si hay que replicar en el servicio de replicación, False caso contrario</param>
        public SocialSearchController(IServiceScopeFactory serviceScope, ConfigService configService, bool pReplicacion, string pUrlServicioEtiquetas)
            : base(serviceScope, configService)
        {
            mReplicacion = pReplicacion;
            mUrlServicioEtiquetas = pUrlServicioEtiquetas;
        }

        #endregion

        #region Metodos generales

        #region publicos

        private void OnShutDownColaMensajes()
        {
            mReiniciarColaMensajes = true;
        }

        private void OnShutDownColaSuscripciones()
        {
            mReiniciarColaSuscripciones = true;
        }

        private void OnShutDownColaComentarios()
        {
            mReiniciarColaComentarios = true;
        }

        private void RealizarMantenimientoRabbitMQColaMensajes(LoggingService pLoggingService)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItemColaTagsMensaje);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDownColaMensajes);

                mClienteRabbitMensajes = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_TAGS_MENSAJES, pLoggingService, mConfigService, EXCHANGE, COLA_TAGS_MENSAJES);

                try
                {
                    mClienteRabbitMensajes.ObtenerElementosDeCola(funcionProcesarItem, funcionShutDown);
                    mReiniciarColaMensajes = false;
                }
                catch (Exception ex)
                {
                    mReiniciarColaMensajes = true;
                    pLoggingService.GuardarLogError(ex);
                }
            }
        }

        private bool ProcesarItemColaTagsMensaje(string pFila)
        {
            using (var scope = ScopedFactory.CreateScope())
            {
                VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                RedisCacheWrapper redisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                ConfigService configService = scope.ServiceProvider.GetRequiredService<ConfigService>();
                IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                ComprobarTraza("SocialSearchGraphGeneration", entityContext, loggingService, redisCacheWrapper, configService, servicesUtilVirtuosoAndReplication);
                try
                {
                    ComprobarCancelacionHilo();

                    Debug.WriteLine($"ProcesarItem, {pFila}!");

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        object[] itemArray = JsonConvert.DeserializeObject<object[]>(pFila);
                        BaseMensajesDS.ColaTagsMensajeRow filaCola = (BaseMensajesDS.ColaTagsMensajeRow)new BaseMensajesDS().ColaTagsMensaje.Rows.Add(itemArray);
                        itemArray = null;

                        ProcesarFilaDeCola(filaCola);

                        filaCola = null;

                        ControladorConexiones.CerrarConexiones(false);

                    }

                    return true;
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLogError(ex);
                    return false;
                }
                finally
                {
                    GuardarTraza(loggingService);
                }
            }
        }

        private void RealizarMantenimientoRabbitMQColaComentario(LoggingService pLoggingService, bool reintentar = true)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItemColaTagsComentarios);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDownColaSuscripciones);

                mClienteRabbitComentarios = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_TAGS_COMENTARIOS, pLoggingService, mConfigService, EXCHANGE, COLA_TAGS_COMENTARIOS);

                try
                {
                    mClienteRabbitComentarios.ObtenerElementosDeCola(funcionProcesarItem, funcionShutDown);
                    mReiniciarColaComentarios = false;
                }
                catch (Exception ex)
                {
                    mReiniciarColaComentarios = true;
                    pLoggingService.GuardarLogError(ex);
                }
            }
        }

        private bool ProcesarItemColaTagsComentarios(string pFila)
        {
            using (var scope = ScopedFactory.CreateScope())
            {
                LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                RedisCacheWrapper redisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                ConfigService configService = scope.ServiceProvider.GetRequiredService<ConfigService>();
                IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                ComprobarTraza("SocialSearchGraphGeneration", entityContext, loggingService, redisCacheWrapper, configService, servicesUtilVirtuosoAndReplication);
                try
                {
                    ComprobarCancelacionHilo();

                    Debug.WriteLine($"ProcesarItem, {pFila}!");

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        object[] itemArray = JsonConvert.DeserializeObject<object[]>(pFila);
                        BaseComentariosDS.ColaTagsComentariosRow filaCola = (BaseComentariosDS.ColaTagsComentariosRow)new BaseComentariosDS().ColaTagsComentarios.Rows.Add(itemArray);
                        itemArray = null;

                        ProcesarFilaDeCola(filaCola);

                        filaCola = null;

                        ControladorConexiones.CerrarConexiones(false);

                    }

                    return true;
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLogError(ex);
                    return false;
                }
                finally
                {
                    GuardarTraza(loggingService);
                }
            }
        }

        private void RealizarMantenimientoRabbitMQColaTagsSuscripciones(LoggingService pLoggingService, bool reintentar = true)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItemColaTagsSuscripciones);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDownColaComentarios);

                mClienteRabbitSuscripciones = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, "ColaTagsSuscripciones", pLoggingService, mConfigService);

                try
                {
                    mClienteRabbitSuscripciones.ObtenerElementosDeCola(funcionProcesarItem, funcionShutDown);
                    mReiniciarColaSuscripciones = false;
                }
                catch (Exception ex)
                {
                    mReiniciarColaSuscripciones = true;
                    pLoggingService.GuardarLogError(ex);
                }
            }
        }

        private bool ProcesarItemColaTagsSuscripciones(string pFila)
        {
            using (var scope = ScopedFactory.CreateScope())
            {
                LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                RedisCacheWrapper redisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                ConfigService configService = scope.ServiceProvider.GetRequiredService<ConfigService>();
                IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                ComprobarTraza("SocialSearchGraphGeneration", entityContext, loggingService, redisCacheWrapper, configService, servicesUtilVirtuosoAndReplication);
                try
                {
                    ComprobarCancelacionHilo();

                    Debug.WriteLine($"ProcesarItem, {pFila}!");

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        object[] itemArray = JsonConvert.DeserializeObject<object[]>(pFila);
                        BaseComentariosDS.ColaTagsComentariosRow filaCola = (BaseComentariosDS.ColaTagsComentariosRow)new BaseComentariosDS().ColaTagsComentarios.Rows.Add(itemArray);
                        itemArray = null;

                        ProcesarFilaDeCola(filaCola);

                        filaCola = null;

                        ControladorConexiones.CerrarConexiones(false);
                    }

                    return true;
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLogError(ex);
                    return false;
                }
                finally
                {
                    GuardarTraza(loggingService);
                }
            }
        }

        /// <summary>
        /// Realiza el mantenimiento del módulo BASE
        /// </summary>
        public override void RealizarMantenimiento(EntityContext pEntityContext, EntityContextBASE pEntityContextBASE, UtilidadesVirtuoso pUtilidadesVirtuoso, LoggingService pLoggingService, RedisCacheWrapper pRedisCacheWrapper, GnossCache pGnossCache, VirtuosoAD pVirtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            DateTime siguienteBorrado = DateTime.Now;
            bool hayElementosPendientes = false;
            ParametroAplicacionCN parametroApliCN = new ParametroAplicacionCN(pEntityContext, pLoggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            //parametroApliCN.InicializarEntityContext();
            GestorParametroAplicacion gestorParametroAplicacion = new GestorParametroAplicacion();
            ParametroAplicacionGBD parametroAplicacionGBD = new ParametroAplicacionGBD(pLoggingService, pEntityContext, mConfigService);
            parametroAplicacionGBD.ObtenerConfiguracionGnoss(gestorParametroAplicacion);
            mUrlIntragnoss = gestorParametroAplicacion.ParametroAplicacion.Where(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).FirstOrDefault().Valor;

            FacetaAD tablaDeConfiguracionCN = new FacetaAD(pLoggingService, pEntityContext, mConfigService, servicesUtilVirtuosoAndReplication);
            FacetadoAD facetadoAD = new FacetadoAD("home", mUrlIntragnoss, pLoggingService, pEntityContext, mConfigService, pVirtuosoAD, servicesUtilVirtuosoAndReplication);
            facetadoAD.ServidoresGrafo = tablaDeConfiguracionCN.ObtenerConfiguracionGrafoConexion();
            tablaDeConfiguracionCN.Dispose();

            #region Establezco el dominio de la cache

            mDominio = gestorParametroAplicacion.ParametroAplicacion.Find(parameteoApp => parameteoApp.Parametro.Equals("UrlIntragnoss")).Valor;
            mDominio = mDominio.Replace("http://", "").Replace("www.", "");

            if (mDominio[mDominio.Length - 1] == '/')
            {
                mDominio = mDominio.Substring(0, mDominio.Length - 1);
            }
            #endregion

            RealizarMantenimientoRabbitMQColaMensajes(pLoggingService);
            RealizarMantenimientoRabbitMQColaComentario(pLoggingService);
            RealizarMantenimientoRabbitMQColaTagsSuscripciones(pLoggingService);
        }

 

        #endregion

        #region privados

        #region Manipulación de relaciones de tags

        /// <summary>
        /// Procesa las filas de blogs
        /// </summary>
        /// <returns>Verdad si ha habido algún error</returns>
        private bool ProcesarFilasDeColaDeInvitaciones()
        {
            bool error = false;

            //Recorro las filas de blogs
            foreach (BaseInvitacionesDS.ColaTagsInvitacionesRow filaCola in mBaseInvitacionesDS.ColaTagsInvitaciones.Rows)
            {
                //Proceso la fila
                error = error || ProcesarFilaDeCola(filaCola);
                ControladorConexiones.CerrarConexiones();
            }

            mBaseInvitacionesDS.Dispose();
            mBaseInvitacionesDS = null;

            return error;
        }

        /// <summary>
        /// Procesa las filas de suscripciones
        /// </summary>
        /// <returns>Verdad si ha habido algún error</returns>
        private bool ProcesarFilasDeColaDeSuscripciones()
        {
            bool error = false;

            //Recorro las filas de blogs
            foreach (BaseSuscripcionesDS.ColaTagsSuscripcionesRow filaCola in mBaseSuscripcionesDS.ColaTagsSuscripciones.Rows)
            {
                //Proceso la fila
                error = error || ProcesarFilaDeCola(filaCola);
                ControladorConexiones.CerrarConexiones();
            }

            mBaseSuscripcionesDS.Dispose();
            mBaseSuscripcionesDS = null;

            return error;
        }

        /// <summary>
        /// Procesa las filas de blogs
        /// </summary>
        /// <returns>Verdad si ha habido algún error</returns>
        private bool ProcesarFilasDeColaDeMensajes()
        {
            bool error = false;

            //Recorro las filas de blogs
            foreach (BaseMensajesDS.ColaTagsMensajeRow filaCola in mBaseMensajesDS.ColaTagsMensaje.Rows)
            {
                //Proceso la fila
                error = error || ProcesarFilaDeCola(filaCola);
                ControladorConexiones.CerrarConexiones();
            }

            mBaseMensajesDS.Dispose();
            mBaseMensajesDS = null;

            return error;
        }

        /// <summary>
        /// Procesa las filas de blogs
        /// </summary>
        /// <returns>Verdad si ha habido algún error</returns>
        private bool ProcesarFilasDeColaDeComentarios()
        {
            bool error = false;

            //Recorro las filas de blogs
            foreach (BaseComentariosDS.ColaTagsComentariosRow filaCola in mBaseComentariosDS.ColaTagsComentarios.Rows)
            {
                //Proceso la fila
                error = error || ProcesarFilaDeCola(filaCola);
                ControladorConexiones.CerrarConexiones();
            }

            mBaseComentariosDS.Dispose();
            mBaseComentariosDS = null;

            return error;
        }

        /// <summary>
        /// Procesa las filas de contactos
        /// </summary>
        /// <returns>Verdad si ha habido algún error</returns>
        private bool ProcesarFilasDeColaDeContactos()
        {
            bool error = false;

            //Recorro las filas de blogs
            foreach (BaseContactosDS.ColaTagsContactoRow filaCola in mBaseContactosDS.ColaTagsContacto.Rows)
            {
                //Proceso la fila
                error = error || ProcesarFilaDeCola(filaCola);
                ControladorConexiones.CerrarConexiones();
            }

            mBaseContactosDS.Dispose();
            mBaseContactosDS = null;

            return error;
        }

        private string PasarObjetoALower(string pObjeto)
        {
            List<string> listaTipos = new List<string>();
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_BLOGS + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_CLASE_UNIVERSIDAD + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_CLASE_SECUNDARIA + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_COMUNIDAD_EDUCATIVA + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_COMUNIDAD_NO_EDUCATIVA + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_COMUNIDADES + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_DAFOS + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_DEBATES + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_ORGANIZACION + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_PERSONA + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_PREGUNTAS + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_ENCUESTAS + "\" .");
            listaTipos.Add("\"" + FacetadoAD.BUSQUEDA_RECURSOS + "\" .");

            if ((!pObjeto.StartsWith("<http://gnoss/")) && (!listaTipos.Contains(pObjeto)))
            {
                return pObjeto.ToLower();
            }
            return pObjeto;
        }

        /// <summary>
        /// Procesa una fila de la cola, calcula sus tags y actualiza la Base de Datos del modelo BASE
        /// </summary>
        /// <param name="pFila">Fila de cola a procesar</param>
        /// <returns>Verdad si ha habido algun error durante la operación</returns>
        private bool ProcesarFilaDeCola(DataRow pFila)
        {
            using (var scope = ScopedFactory.CreateScope())
            {

                EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                EntityContextBASE entityContextBase = scope.ServiceProvider.GetRequiredService<EntityContextBASE>();
                LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();

                bool error = false;

                try
                {
                    string tripletasMensajesFrom = "";
                    string tripletasMensajesTo = "";
                    string tripletasComentarios = "";
                    string tripletasInvitaciones = "";
                    string tripletasSuscripciones = "";
                    string tripletasContactos = "";

                    short estado = (short)pFila["Estado"];
                    if (estado < 2)
                    {
                        //Obtengo los tags en una lista
                        Dictionary<short, List<string>> listaTagsFiltros = ObtenerTagsFiltros(pFila["Tags"].ToString(), pFila.Table.DataSet);

                        if (pFila["Tipo"].Equals((short)TiposElementosEnCola.Agregado))
                        {
                            Guid id = Guid.Empty;
                            string idString = "";
                            string idfrom = "";
                            string[] idsTo = null;
                            string[] idsperfil = null;

                            string iddesinv = "";
                            string iddessus = "";
                            string idsusrec = "";
                            Dictionary<string, string> listaIdsEliminar = new Dictionary<string, string>();

                            ActualizacionFacetadoCN actualizacionFacetadoCN = new ActualizacionFacetadoCN(mUrlIntragnoss, entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                            FacetaDS tConfiguracion = new FacetaDS();

                            //FacetadoAD facetadoAD = null;

                            ProyectoCN proyectoCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                            Guid proyid = ProyectoAD.MetaProyecto;
                            short estadoProy = 2;
                            if ((int)pFila["TablaBaseProyectoID"] != 0)
                            {
                                Es.Riam.Gnoss.AD.EntityModel.Models.ProyectoDS.Proyecto filaProy = proyectoCN.ObtenerProyectoPorTablaBaseProyectoID((int)pFila["TablaBaseProyectoID"]).ListaProyecto.FirstOrDefault();

                                proyid = filaProy.ProyectoID;
                                estadoProy = filaProy.Estado;
                                bool esProyectoPrivado = filaProy.TipoAcceso.Equals((short)TipoAcceso.Privado) || filaProy.TipoAcceso.Equals((short)TipoAcceso.Reservado);
                            }

                            string proyidString = proyid.ToString();

                            #region Actualizar facetado

                            List<string> tags = new List<string>();
                            string valorSearch = "";

                            string ficheroConfiguracion = "";
                            string tablaReplica = "";

                            if (pFila.Table.DataSet is BaseMensajesDS)
                            {
                                //facetadoAD = new FacetadoAD(mFicheroConfiguracionHomeBD, false, mUrlIntragnoss, "ColaReplicaciónMasterHome", mReplicacion);
                                //facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                                ficheroConfiguracion = mFicheroConfiguracionHomeBD;
                                tablaReplica = ReplicacionAD.COLA_REPLICACION_MASTER_HOME;

                                #region Mensajes

                                idString = listaTagsFiltros[(short)TiposTags.IDTagMensaje][0].ToUpper();
                                id = new Guid(idString);
                                idfrom = listaTagsFiltros[(short)TiposTags.IDTagMensajeFrom][0].ToUpper();
                                idsTo = (listaTagsFiltros[(short)TiposTags.IDTagMensajeTo][0]).Split(new string[] { "|" }, StringSplitOptions.RemoveEmptyEntries);

                                List<Guid> listaIdsGrupos = new List<Guid>();
                                List<Guid> listaParticipantesGrupos = new List<Guid>();
                                bool hayGrupos = false;

                                foreach (string idTo in idsTo)
                                {
                                    if (idTo.StartsWith("g_"))
                                    {
                                        //Compruebo si hay algun grupo, si no hay, me ahorro una carga
                                        hayGrupos = true;
                                    }
                                    else
                                    {
                                        //Los añado en la lista, asi si estan en un grupo, no les mando el mensajes dos veces
                                        listaParticipantesGrupos.Add(new Guid(idTo));
                                    }
                                }

                                if (hayGrupos)
                                {
                                    int numCorreosDS = 0;

                                    CorreoCN correoCN = new CorreoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                                    CorreoDS correoDS = correoCN.ObtenerCorreoPorID(id, new Guid(idfrom), null);

                                    NotificacionCN notificacionCN = new NotificacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

                                    CorreoDS.CorreoInternoRow filaCorreo = (CorreoDS.CorreoInternoRow)correoDS.CorreoInterno.Rows[0];

                                    GestionNotificaciones GestorNotificaciones = new GestionNotificaciones(new DataWrapperNotificacion(), loggingService, entityContext, mConfigService, servicesUtilVirtuosoAndReplication);

                                    Guid CorreoID = filaCorreo.CorreoID;
                                    Guid Autor = filaCorreo.Autor;
                                    string Asunto = filaCorreo.Asunto;
                                    string Cuerpo = filaCorreo.Cuerpo;
                                    DateTime Fecha = filaCorreo.Fecha;
                                    string DestinatariosID = filaCorreo.DestinatariosID;
                                    string DestinatariosNombres = filaCorreo.DestinatariosNombres;
                                    Guid ConversacionID = filaCorreo.ConversacionID;

                                    ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                                    GestionProyecto gestProy = new GestionProyecto(proyCN.ObtenerProyectoCargaLigeraPorID(ProyectoAD.MetaProyecto), loggingService, entityContext);
                                    Proyecto Proyecto = gestProy.ListaProyectos[ProyectoAD.MetaProyecto];

                                    IdentidadCN identCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                                    List<Guid> listaAutor = new List<Guid>();
                                    listaAutor.Add(Autor);
                                    string nombreAutor = identCN.ObtenerNombresDeIdentidades(listaAutor)[Autor];

                                    foreach (string idTo in idsTo)
                                    {
                                        if (idTo.StartsWith("g_"))
                                        {
                                            try
                                            {
                                                Guid idGrupo = new Guid(idTo.Substring(2));
                                                if (!listaIdsGrupos.Contains(idGrupo))
                                                {
                                                    listaIdsGrupos.Add(idGrupo);

                                                    List<Guid> identidadesGrupo = identCN.ObtenerParticipantesGrupo(idGrupo);

                                                    DataWrapperIdentidad identidadesParticipantes = identCN.ObtenerIdentidadesPorID(identidadesGrupo, false);
                                                    GestionIdentidades gestIdentidades = new GestionIdentidades(identidadesParticipantes, loggingService, entityContext, mConfigService, servicesUtilVirtuosoAndReplication);

                                                    PersonaCN personaCN = new PersonaCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                                                    var perfiles = gestIdentidades.ListaPerfiles.Where(item => item.Value.PersonaID.HasValue);
                                                    if (perfiles.Any())
                                                    {
                                                        gestIdentidades.GestorPersonas = new GestionPersonas(personaCN.ObtenerPersonasPorID(perfiles.Select(item => item.Value.PersonaID.Value).ToList()), loggingService, entityContext);
                                                    }

                                                    foreach (Guid idParticipante in identidadesGrupo)
                                                    {
                                                        if (!listaParticipantesGrupos.Contains(idParticipante))
                                                        {
                                                            try
                                                            {
                                                                listaParticipantesGrupos.Add(idParticipante);

                                                                CorreoDS.CorreoInternoRow nuevaFilaCorreoRecibido = correoDS.CorreoInterno.NewCorreoInternoRow();
                                                                nuevaFilaCorreoRecibido.CorreoID = CorreoID;
                                                                nuevaFilaCorreoRecibido.Autor = Autor;
                                                                nuevaFilaCorreoRecibido.Destinatario = idParticipante;
                                                                nuevaFilaCorreoRecibido.Asunto = Asunto;
                                                                nuevaFilaCorreoRecibido.Cuerpo = Cuerpo;
                                                                nuevaFilaCorreoRecibido.Fecha = Fecha;
                                                                nuevaFilaCorreoRecibido.Leido = false;
                                                                nuevaFilaCorreoRecibido.Eliminado = false;
                                                                nuevaFilaCorreoRecibido.EnPapelera = false;
                                                                nuevaFilaCorreoRecibido.DestinatariosID = DestinatariosID;
                                                                nuevaFilaCorreoRecibido.DestinatariosNombres = DestinatariosNombres;
                                                                nuevaFilaCorreoRecibido.ConversacionID = ConversacionID;

                                                                correoDS.CorreoInterno.AddCorreoInternoRow(nuevaFilaCorreoRecibido);

                                                                if (gestIdentidades.ListaIdentidades.ContainsKey(idParticipante))
                                                                {
                                                                    GestorNotificaciones.AgregarNotificacionAvisoNuevoCorreo(nombreAutor, gestIdentidades.ListaIdentidades[idParticipante], Cuerpo, Asunto, TiposNotificacion.AvisoCorreoNuevoContacto, CorreoID, Proyecto.FilaProyecto.URLPropia, Proyecto, "es", false);

                                                                }

                                                                if (numCorreosDS > 50)
                                                                {
                                                                    correoCN.ActualizarCorreo(correoDS);
                                                                    entityContext.SaveChanges();
                                                                    correoDS.Clear();
                                                                    numCorreosDS = 0;
                                                                    Thread.Sleep(1000);
                                                                }

                                                                numCorreosDS++;
                                                            }
                                                            catch
                                                            { }
                                                        }
                                                    }
                                                }
                                            }
                                            catch
                                            { }
                                        }
                                    }

                                    notificacionCN.ActualizarNotificacion();

                                    correoCN.ActualizarCorreo(correoDS);
                                    entityContext.SaveChanges();
                                    correoDS.Clear();

                                    idsTo = Array.ConvertAll(listaParticipantesGrupos.ToArray(), x => x.ToString());
                                }

                                string descripcion = "";
                                string tituloOriginal = actualizacionFacetadoCN.ObtenerTituloMensaje(tConfiguracion, id, idfrom, out descripcion);

                                //limpiar el titulo de RE: y Fwd:
                                string titulo = UtilCadenas.LimpiarAsunto(tituloOriginal);

                                valorSearch += " " + UtilCadenas.EliminarHtmlDeTextoPorEspacios(tituloOriginal);
                                valorSearch += " " + UtilCadenas.EliminarHtmlDeTextoPorEspacios(descripcion);

                                string remitente = "";

                                if (idfrom != Guid.Empty.ToString())
                                {
                                    IdentidadCN identidadCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                                    GestionIdentidades gesdtorIden = new GestionIdentidades(identidadCN.ObtenerIdentidadPorID(new Guid(idfrom), false), loggingService, entityContext, mConfigService, servicesUtilVirtuosoAndReplication);
                                    identidadCN.Dispose();
                                    remitente = gesdtorIden.ListaIdentidades[new Guid(idfrom)].Nombre(new Guid(idfrom));
                                }
                                else
                                {
                                    remitente = "GNOSS";
                                }

                                valorSearch += " " + remitente;

                                string triplesDescompuestosTitulo = UtilidadesVirtuoso.AgregarTripletasDescompuestasTitulo(id.ToString(), "<http://gnoss/hasTagDesc>", titulo);

                                //tags etiquetas descompuestos titulo
                                tripletasMensajesFrom += triplesDescompuestosTitulo;

                                tripletasMensajesTo += triplesDescompuestosTitulo;

                                //string triplesDescompuestosDestinatarios = actualizacionFacetadoCN.AgregarTripletasDescompuestasTitulo(id, "11111111-1111-1111-1111-111111111111", "<http://gnoss/hasTagDesc>", destinatarios);

                                ////tags etiquetas descompuestos destinatarios
                                //tripletasMensajesFrom += triplesDescompuestosDestinatarios;

                                //tripletasMensajesTo += triplesDescompuestosDestinatarios;

                                //tags etiquetas descompuestos remintente


                                tripletasMensajesTo += UtilidadesVirtuoso.AgregarTripletasDescompuestasTitulo(id.ToString(), "<http://gnoss/hasTagDesc>", remitente);

                                tripletasMensajesTo += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">", "<http://gnoss/hasTagDesc>", "\"" + remitente.ToLower() + "\"");
                                try
                                {
                                    CallEtiquetadoAutomaticoService servicioEtiquetas = new CallEtiquetadoAutomaticoService(mConfigService);

                                    string etiquetas = servicioEtiquetas.SeleccionarEtiquetasDesdeServicio(titulo, descripcion, "11111111-1111-1111-1111-111111111111");

                                    string[] etiquetasA = (etiquetas).Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
                                    List<string> etiquetasList = new List<string>();
                                    foreach (string etiqueta in etiquetasA)
                                    {
                                        etiquetasList.Add(etiqueta);
                                    }

                                    foreach (string tag in etiquetasList)
                                    {
                                        string objeto = "\"" + tag.Replace("\"", "'").Replace("\r\n", " ").Replace("\n", " ").Trim();

                                        if (!string.IsNullOrEmpty(objeto))
                                        {
                                            tags.Add(objeto + "\"");

                                            tripletasMensajesFrom += UtilidadesVirtuoso.AgregarTripletasDescompuestasTitulo(id.ToString(), "<http://gnoss/hasTagDesc>", objeto);

                                            tripletasMensajesTo += UtilidadesVirtuoso.AgregarTripletasDescompuestasTitulo(id.ToString(), "<http://gnoss/hasTagDesc>", objeto);
                                            objeto += "\" .";

                                            tripletasMensajesFrom += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">", "<http://rdfs.org/sioc/types#Tag>", PasarObjetoALower(objeto));
                                            tripletasMensajesTo += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">", "<http://rdfs.org/sioc/types#Tag>", PasarObjetoALower(objeto));
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    loggingService.GuardarLogError(ex);
                                }

                                //Inserto información del tipo
                                tripletasMensajesFrom += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">",
                                 "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                                 "\"Mensaje\"");


                                tripletasMensajesTo += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">",
                                 "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                                 "\"Mensaje\"");

                                //obtenemos información extra para los destinatarios
                                actualizacionFacetadoCN.ObtieneInformacionExtraMensajesTo(tConfiguracion, idString, idsTo[0]);
                                foreach (DataRow myrow in tConfiguracion.Tables["TripletasMensajesTo"].Rows)
                                {
                                    string objeto = (string)myrow[2];
                                    string predicado = (string)myrow[1];
                                    tripletasMensajesTo += FacetadoAD.GenerarTripleta((string)myrow[0], predicado, objeto);
                                }
                                tConfiguracion.Clear();
                                //obtenemos información extra para los remitentes
                                actualizacionFacetadoCN.ObtieneInformacionExtraMensajesFrom(tConfiguracion, idString, idfrom);

                                foreach (DataRow myrow in tConfiguracion.Tables["tripletasMensajesFrom"].Rows)
                                {
                                    string objeto = (string)myrow[2];
                                    tripletasMensajesFrom += FacetadoAD.GenerarTripleta((string)myrow[0], (string)myrow[1], objeto);
                                }
                                tConfiguracion.Clear();

                                //obtenemos información extra para los remitentes sobre los destinatarios
                                actualizacionFacetadoCN.ObtieneInformacionExtraMensajesFromObtenerTo(tConfiguracion, idString, idfrom);
                                foreach (DataRow myrow in tConfiguracion.Tables["TripletasMensajesFromObtenerTo"].Rows)
                                {
                                    //string objeto = (string)myrow[2];

                                    string[] nombresdestinatarios = ((string)myrow[2]).Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);

                                    foreach (string destinatario in nombresdestinatarios)
                                    {
                                        string objetosincomillas = destinatario;
                                        if ((destinatario.StartsWith("\"")) || (destinatario.EndsWith("\"")))
                                        {
                                            objetosincomillas = destinatario.Replace("\"", "");
                                        }
                                        if (!objetosincomillas.Equals(" ."))
                                        {
                                            tripletasMensajesFrom += FacetadoAD.GenerarTripleta((string)myrow[0], (string)myrow[1], "\"" + objetosincomillas.ToLower() + "\"");

                                            tripletasMensajesFrom += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">", "<http://gnoss/hasTagDesc>", "\"" + objetosincomillas.ToLower() + "\"");

                                            //tripletasMensajesTo += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">", "<http://gnoss/hasTagDesc>", "\"" + objetosincomillas.ToLower() + "\"");

                                            valorSearch += " " + objetosincomillas;
                                        }
                                    }
                                }
                                tConfiguracion.Clear();

                                #endregion
                            }
                            else if (pFila.Table.DataSet is BaseSuscripcionesDS)
                            {
                                //facetadoAD = new FacetadoAD(mFicheroConfiguracionHomeBD, false, mUrlIntragnoss, "ColaReplicaciónMasterHome", mReplicacion);
                                //facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                                ficheroConfiguracion = mFicheroConfiguracionHomeBD;
                                tablaReplica = ReplicacionAD.COLA_REPLICACION_MASTER_HOME;

                                #region Suscripciones

                                idString = listaTagsFiltros[(short)TiposTags.IDTagSuscripcion][0].ToUpper();
                                id = new Guid(idString);

                                idsusrec = listaTagsFiltros[(short)TiposTags.IDTagSuscripcionRecurso][0].ToUpper();
                                iddessus = listaTagsFiltros[(short)TiposTags.IDTagSuscripcionPerfil][0].ToUpper();

                                string titulo = actualizacionFacetadoCN.ObtenerTituloSuscripcion(id, new Guid(idsusrec));
                                valorSearch += " " + UtilCadenas.EliminarHtmlDeTextoPorEspacios(titulo);

                                tripletasSuscripciones += UtilidadesVirtuoso.AgregarTripletasDescompuestasTitulo(idString + "_" + idsusrec, "<http://gnoss/hasTagDesc>", titulo);

                                List<string> listaTags = actualizacionFacetadoCN.ObtenerTags(new Guid(idsusrec), "Recurso", ProyectoAD.MetaProyecto);
                                if (listaTags.Count == 0)
                                {
                                    listaTags = actualizacionFacetadoCN.ObtenerTags(new Guid(idsusrec), "EntradaBlog", ProyectoAD.MetaProyecto);
                                }

                                string sujeto = "<http://gnoss/" + idString + "_" + idsusrec.ToUpper() + ">";

                                foreach (string tag in listaTags)
                                {
                                    string objeto = "\"" + tag.Replace("\"", "'").Trim() + "\" .";
                                    tripletasSuscripciones += FacetadoAD.GenerarTripleta(sujeto, "<http://gnoss/hasTagDesc>", objeto);
                                }

                                tConfiguracion.Clear();

                                //Inserto información del tipo
                                tripletasSuscripciones += FacetadoAD.GenerarTripleta(sujeto, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "\"Suscripcion\"");

                                //obtenemos información extra para los destinatarios
                                List<QueryTriples> listaInformacionExtraSuscriptores = actualizacionFacetadoCN.ObtieneInformacionExtraSuscripciones(id, new Guid(idsusrec));

                                foreach (QueryTriples query in listaInformacionExtraSuscriptores)
                                {
                                    tripletasSuscripciones += FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, query.Objeto);
                                }
                                tConfiguracion.Clear();


                                #endregion
                            }
                            else if (pFila.Table.DataSet is BaseInvitacionesDS)
                            {
                                //facetadoAD = new FacetadoAD(mFicheroConfiguracionHomeBD, false, mUrlIntragnoss, "ColaReplicaciónMasterHome", mReplicacion);
                                //facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                                ficheroConfiguracion = mFicheroConfiguracionHomeBD;
                                tablaReplica = ReplicacionAD.COLA_REPLICACION_MASTER_HOME;

                                #region Invitaciones

                                idString = listaTagsFiltros[(short)TiposTags.IDTagInvitacion][0].ToUpper();
                                id = new Guid(idString);
                                iddesinv = listaTagsFiltros[(short)TiposTags.IDTagInvitacionIdDestino][0].ToUpper();

                                //Inserto información del tipo
                                tripletasInvitaciones += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">",
                                 "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                                 "\"Invitacion\"");

                                //obtenemos información extra para los destinatarios
                                List<QueryTriples> listaInformacionExtraInvitaciones = actualizacionFacetadoCN.ObtieneInformacionExtraInvitaciones(idString);

                                foreach (QueryTriples query in listaInformacionExtraInvitaciones)
                                {
                                    tripletasInvitaciones += FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, query.Objeto);
                                }
                                tConfiguracion.Clear();

                                #endregion
                            }
                            else if (pFila.Table.DataSet is BaseComentariosDS)
                            {
                                //facetadoAD = new FacetadoAD(mFicheroConfiguracionHomeBD, false, mUrlIntragnoss, "ColaReplicaciónMasterHome", mReplicacion);
                                //facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                                ficheroConfiguracion = mFicheroConfiguracionHomeBD;
                                tablaReplica = ReplicacionAD.COLA_REPLICACION_MASTER_HOME;

                                #region Comentarios

                                //ActualizacionFacetadoCN actualizacionFacetadoCN = new ActualizacionFacetadoCN(mFicheroConfiguracionBD, false, mUrlIntragnoss);

                                idString = listaTagsFiltros[(short)TiposTags.IDTagComentario][0].ToUpper();
                                id = new Guid(idString);

                                idsperfil = listaTagsFiltros[(short)TiposTags.IDsTagComentarioPerfil][0].Split(new string[] { "|" }, StringSplitOptions.RemoveEmptyEntries);

                                string titulo = actualizacionFacetadoCN.ObtenerTituloComentario(id);

                                valorSearch += " " + UtilCadenas.EliminarHtmlDeTextoPorEspacios(titulo);

                                //tags etiquetas descompuestos titulo
                                tripletasComentarios += UtilidadesVirtuoso.AgregarTripletasDescompuestasTitulo(id.ToString(), "<http://gnoss/hasTagDesc>", titulo);

                                //Inserto información del tipo
                                tripletasComentarios += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">",
                                 "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                                 "\"Comentario\"");

                                //obtenemos información extra para los destinatarios
                                List<QueryTriples> listaTripletasComentarios = actualizacionFacetadoCN.ObtieneInformacionExtraComentarios(id);

                                foreach (QueryTriples query in listaTripletasComentarios)
                                {
                                    tripletasComentarios += FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, query.Objeto);
                                }

                                tConfiguracion.Clear();

                                #endregion
                            }
                            else if (pFila.Table.DataSet is BaseContactosDS)
                            {
                                //facetadoAD = new FacetadoAD(mFicheroConfiguracionBD, false, mUrlIntragnoss, "ColaReplicaciónMasterHome", mReplicacion);
                                //facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                                ficheroConfiguracion = mFicheroConfiguracionBD;
                                tablaReplica = ReplicacionAD.COLA_REPLICACION_MASTER;

                                #region Contactos

                                //AYUDA JUAN
                                idString = listaTagsFiltros[(short)TiposTags.IDTagContacto][0].ToUpper();
                                id = new Guid(idString);
                                string idamigo = listaTagsFiltros[(short)TiposTags.IDTagIdentidad][0].ToUpper();


                                string titulo = actualizacionFacetadoCN.ObtenerTituloContacto(id);
                                tripletasContactos += UtilidadesVirtuoso.AgregarTripletasDescompuestasTitulo(id.ToString(), "<http://gnoss/hasTagTituloDesc>", titulo);

                                valorSearch += " " + UtilCadenas.EliminarHtmlDeTextoPorEspacios(titulo);

                                //obtenemos información extra de amigos
                                actualizacionFacetadoCN.ObtieneInformacionExtraContactos(idString, idamigo);

                                foreach (DataRow myrow in tConfiguracion.Tables["TripletasContactos"].Rows)
                                {
                                    string objeto = (string)myrow[2];
                                    string predicado = (string)myrow[1];
                                    if (!predicado.Contains("type"))
                                    {
                                        tripletasContactos += FacetadoAD.GenerarTripleta((string)myrow[0], predicado, PasarObjetoALower(objeto));
                                    }
                                    else
                                    {
                                        tripletasContactos += FacetadoAD.GenerarTripleta((string)myrow[0], predicado, objeto);
                                    }
                                }
                                tConfiguracion.Clear();

                                #endregion
                            }

                            string destinatarios = "";
                            if (idsTo != null && idsTo.Length > 1)
                            {
                                destinatarios = actualizacionFacetadoCN.ObtenerDestinatarioMensaje(tConfiguracion, idString, idfrom);

                                string triplesDescompuestosDestinatarios = UtilidadesVirtuoso.AgregarTripletasDescompuestasTitulo(id.ToString(), "<http://gnoss/hasTagDesc>", destinatarios);
                                tripletasMensajesFrom += triplesDescompuestosDestinatarios;
                            }

                            foreach (string tag in tags)
                            {
                                valorSearch += " " + tag;
                            }

                            valorSearch = "\"" + valorSearch.Replace("\"", "'").Trim() + "\" .";
                            valorSearch = UtilCadenas.EliminarHtmlDeTextoPorEspacios(valorSearch).Replace("\\", "/");

                            string tripleSearch = FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">", "<http://gnoss/search>", UtilidadesVirtuoso.RemoverSignosAcentos(valorSearch));

                            //Tripletas de mensajes
                            if (!string.IsNullOrEmpty(tripletasMensajesFrom))
                            {
                                if (!listaIdsEliminar.ContainsKey(idString))
                                {
                                    listaIdsEliminar.Add(idString, "");
                                }
                                //Inserto información de la identidad
                                tripletasMensajesFrom += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">",
                                 "<http://gnoss/IdentidadID>",
                                 "<http://gnoss/" + idfrom.ToUpper() + ">");

                                Guid usuarioID = actualizacionFacetadoCN.ObtenerIdUsuarioDesdeIdentidad(new Guid(idfrom));

                                tripletasMensajesFrom += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">",
                                "<http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#isRead>",
                                "\"Leidos\"");

                                tConfiguracion.Clear();

                                //facetadoAD.InsertaTripletasConModify(usuario, tripletasMensajesFrom, listaIdsEliminar);
                                tripletasMensajesFrom += tripleSearch;

                                InsertaTripletasConModify_ControlCheckPoint(entityContext, loggingService, virtuosoAD, tablaReplica, usuarioID.ToString(), tripletasMensajesFrom, listaIdsEliminar, servicesUtilVirtuosoAndReplication);
                            }

                            if (!string.IsNullOrEmpty(tripletasMensajesTo))
                            {
                                if (!listaIdsEliminar.ContainsKey(idString))
                                {
                                    listaIdsEliminar.Add(idString, "");
                                }

                                foreach (string idTo in idsTo)
                                {   //Inserto información de la identidad
                                    String mTripletasMensajesToAux = tripletasMensajesTo + FacetadoAD.GenerarTripleta("<http://gnoss/" + id.ToString().ToUpper() + ">",
                                     "<http://gnoss/IdentidadID>",
                                      "<http://gnoss/" + idTo.ToUpper() + ">");


                                    //Tripletas leido o no leido
                                    actualizacionFacetadoCN.ObtieneInformacionMensajesLeidoNoLeido(tConfiguracion, idString, idTo);

                                    foreach (DataRow myrow in tConfiguracion.Tables["TripletasMensajesLeido"].Rows)
                                    {
                                        string objeto = (string)myrow[2];
                                        mTripletasMensajesToAux += FacetadoAD.GenerarTripleta((string)myrow[0], (string)myrow[1], objeto);
                                    }
                                    tConfiguracion.Clear();

                                    if (idsTo.Length > 1)
                                    {
                                        //quitar al actual destinatario
                                        Guid UsuarioActualID = new Guid(idTo);
                                        string destinatarioActual = actualizacionFacetadoCN.ObtenerNombrePerfilIdentidad(UsuarioActualID);
                                        string destinatariosMenosActual = destinatarios.Replace(destinatarioActual, "");
                                        string triplesDescompuestosDestinatarios = UtilidadesVirtuoso.AgregarTripletasDescompuestasTitulo(id.ToString(), "<http://gnoss/hasTagDesc>", destinatariosMenosActual);

                                        char[] separador = { ',' };
                                        string[] destinatariosSeparados = destinatariosMenosActual.Split(separador, StringSplitOptions.RemoveEmptyEntries);
                                        foreach (string destinatario in destinatariosSeparados)
                                        {
                                            mTripletasMensajesToAux += FacetadoAD.GenerarTripleta("<http://gnoss/" + id.ToString().ToUpper() + ">",
                                     "<http://gnoss/hasTagDesc>", "\"" + destinatario.ToLower().Replace("\"", "") + "\"");
                                        }

                                        //tags etiquetas descompuestos destinatarios
                                        mTripletasMensajesToAux += triplesDescompuestosDestinatarios;
                                    }
                                    Guid usuarioID = actualizacionFacetadoCN.ObtenerIdUsuarioDesdeIdentidad(new Guid(idTo));
                                    //facetadoAD.InsertaTripletasConModify(usuario, mTripletasMensajesToAux, listaIdsEliminar);
                                    mTripletasMensajesToAux += tripleSearch;

                                    InsertaTripletasConModify_ControlCheckPoint(entityContext, loggingService, virtuosoAD, tablaReplica, usuarioID.ToString(), mTripletasMensajesToAux, listaIdsEliminar, servicesUtilVirtuosoAndReplication);
                                }
                            }

                            //Una vez se ha insertado en virtuoso los datos de los destinatarios y los que reciben el mensaje, inserto en la cola refrescocaché.
                            if (!string.IsNullOrEmpty(tripletasMensajesTo) || !string.IsNullOrEmpty(tripletasMensajesFrom))
                            {
                                //Enviamos una fila al servicio windows encargado del refresco de la caché para que muestre el mensaje que se ha recibido.
                                BaseComunidadCN baseComunidadCN = new BaseComunidadCN(entityContext, loggingService, entityContextBase, mConfigService, servicesUtilVirtuosoAndReplication);

                                try
                                {
                                    if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
                                    {
                                        baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(ProyectoAD.MetaProyecto, TiposEventosRefrescoCache.CambiosBandejaDeMensajes, TipoBusqueda.Mensajes, pFila["Tags"].ToString());
                                    }
                                    else
                                    {
                                        baseComunidadCN.InsertarFilaEnColaRefrescoCache(ProyectoAD.MetaProyecto, TiposEventosRefrescoCache.CambiosBandejaDeMensajes, TipoBusqueda.Mensajes, pFila["Tags"].ToString());
                                    }
                                }
                                catch (Exception ex)
                                {
                                    loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                    baseComunidadCN.InsertarFilaEnColaRefrescoCache(ProyectoAD.MetaProyecto, TiposEventosRefrescoCache.CambiosBandejaDeMensajes, TipoBusqueda.Mensajes, pFila["Tags"].ToString());
                                }

                                baseComunidadCN.Dispose();

                                tripletasMensajesTo = "";
                                tripletasMensajesFrom = "";
                            }

                            if (!string.IsNullOrEmpty(tripletasComentarios))
                            {
                                if (!listaIdsEliminar.ContainsKey(idString))
                                {
                                    listaIdsEliminar.Add(idString, "");
                                }
                                LiveCN liveCN = new LiveCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

                                foreach (string idperfil in idsperfil)
                                {

                                    //Grafo del perfil a insertar
                                    string TripletasComentariosPerfil = tripletasComentarios + FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">",
                                    "<http://gnoss/PerfilID>",
                                     "<http://gnoss/" + idperfil.ToUpper() + ">");


                                    //Tripletas leido o no leido
                                    TripletasComentariosPerfil += FacetadoAD.GenerarTripleta("<http://gnoss/" + idString + ">", "<http://gnoss/Leido>", "\"Pendientes de leer\"");

                                    string usuario = actualizacionFacetadoCN.obtenerIdusuarioDesdePerfil(new Guid(idperfil)).ToString().ToLower();

                                    // ***************************************************************
                                    //Si es un comentario nuevo, aumentar el contador
                                    //Si es un comentario editado y el perfil no ha leído el comentario todavía, aumentar el contador
                                    if (ComentarioNoExisteOHaSidoLeidoPerfil_ControlCheckPoint(entityContext, loggingService, virtuosoAD, idString, usuario, servicesUtilVirtuosoAndReplication))
                                    {
                                        liveCN.AumentarContadorNuevosComentarios(new Guid(idperfil));
                                    }

                                    //foreach (DataRow myrow in tConfiguracion.Tables["TripletasComentariosLeido"].Rows)
                                    //{
                                    //    string objeto = (string)myrow[2];
                                    //    TripletasComentariosPerfil += FacetadoAD.GenerarTripleta((string)myrow[0], (string)myrow[1], objeto);
                                    //}
                                    //tConfiguracion.Clear();

                                    //facetadoAD.InsertaTripletasConModify(usuario, TripletasComentariosPerfil, listaIdsEliminar);
                                    TripletasComentariosPerfil += tripleSearch;

                                    InsertaTripletasConModify_ControlCheckPoint(entityContext, loggingService, virtuosoAD, tablaReplica, usuario, TripletasComentariosPerfil, listaIdsEliminar, servicesUtilVirtuosoAndReplication);
                                }
                                liveCN.Dispose();

                                tripletasComentarios = "";
                            }


                            if (!string.IsNullOrEmpty(tripletasInvitaciones))
                            {
                                if (!listaIdsEliminar.ContainsKey(idString))
                                {
                                    listaIdsEliminar.Add(idString, "");
                                }
                                //Grafo del perfil a insertar
                                tripletasInvitaciones += FacetadoAD.GenerarTripleta("<http://gnoss/" + id.ToString().ToUpper() + ">",
                                    "<http://gnoss/IdentidadID>",
                                     "<http://gnoss/" + iddesinv.ToUpper() + ">");

                                Guid usuarioID = actualizacionFacetadoCN.ObtenerIdUsuarioDesdeIdentidad(new Guid(iddesinv));
                                //facetadoAD.InsertaTripletasConModify(usuario, tripletasInvitaciones, listaIdsEliminar);
                                tripletasInvitaciones += tripleSearch;

                                InsertaTripletasConModify_ControlCheckPoint(entityContext, loggingService, virtuosoAD, tablaReplica, usuarioID.ToString(), tripletasInvitaciones, listaIdsEliminar, servicesUtilVirtuosoAndReplication);

                                tripletasInvitaciones = "";
                            }


                            if (!string.IsNullOrEmpty(tripletasSuscripciones))
                            {
                                if (!listaIdsEliminar.ContainsKey(idString))
                                {
                                    listaIdsEliminar.Add(idString, "");
                                }
                                tripletasSuscripciones += FacetadoAD.GenerarTripleta("<http://gnoss/" + id.ToString().ToUpper() + "_" + idsusrec + ">", "<http://gnoss/PerfilID>", "<http://gnoss/" + iddessus.ToUpper() + ">");

                                string usuario = actualizacionFacetadoCN.obtenerIdusuarioDesdePerfil(new Guid(iddessus)).ToString().ToLower();
                                //facetadoAD.InsertaTripletasConModify(usuario, tripletasSuscripciones, listaIdsEliminar);
                                tripletasSuscripciones += tripleSearch;

                                InsertaTripletasConModify_ControlCheckPoint(entityContext, loggingService, virtuosoAD, tablaReplica, usuario, tripletasSuscripciones, listaIdsEliminar, servicesUtilVirtuosoAndReplication);

                                tripletasSuscripciones = "";
                            }

                            if (!string.IsNullOrEmpty(tripletasContactos))
                            {
                                //facetadoAD.InsertaTripletas("contactos", tripletasContactos, 0);
                                InsertaTripletas_ControlCheckPoint(entityContext, loggingService, virtuosoAD, tablaReplica, "contactos", tripletasContactos, 0, servicesUtilVirtuosoAndReplication);
                                tripletasContactos = "";
                            }

                            listaIdsEliminar.Clear();

                            #endregion

                            ////Actualización del índice SEARCH.
                            //if (!string.IsNullOrEmpty(valorSearch))
                            //{
                            //    UtilidadesVirtuoso.ActualizarIndiceSearch_ControlCheckPoint(ficheroConfiguracion, mFicheroConfiguracionBDBase, mUrlIntragnoss, tablaReplica);
                            //}
                        }

                        pFila["Estado"] = (short)EstadosColaTags.Procesado;
                    }
                }
                catch (Exception exFila)
                {
                    ControladorConexiones.CerrarConexiones();
                    //Ha habido algún error durante la operación, notifico el error
                    error = true;

                    string mensaje = "Excepción: " + exFila.ToString() + "\n\n\tTraza: " + exFila.StackTrace + "\n\nFila: " + pFila["OrdenEjecucion"];
                    loggingService.GuardarLog("ERROR:  " + mensaje);

                    pFila["Estado"] = ((short)pFila["Estado"]) + 1; //Aumento en 1 el error, cuando llegue a 4 no se volverá a intentar

                    // Se envía al visor de sucesos una notificación
                    try
                    {
                        string sSource;
                        string sLog;
                        string sEvent;

                        sSource = "Servicio_Modulo_BASE";
                        sLog = "Servicios_GNOSS";
                        sEvent = mensaje;

                        if (!EventLog.SourceExists(sSource))
                            EventLog.CreateEventSource(sSource, sLog);

                        EventLog.WriteEntry(sSource, sEvent, EventLogEntryType.Warning, 888);
                    }
                    catch (Exception) { }
                }
                finally
                {
                    pFila["FechaProcesado"] = DateTime.Now;
                }

                return error;
            }
        }

        private bool ComentarioNoExisteOHaSidoLeidoPerfil_ControlCheckPoint(EntityContext pEntityContext, LoggingService pLoggingService, VirtuosoAD pVirtuosoAD, string pComentarioId, string pUsuarioId, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoCN facCN = null;
            bool comentarioLeido = false;
            try
            {
                facCN = new FacetadoCN(mUrlIntragnoss, "", pEntityContext, pLoggingService, mConfigService, pVirtuosoAD, servicesUtilVirtuosoAndReplication);

                comentarioLeido = facCN.ComprobarSiComentarioNoExisteOHaSidoLeidoPerfil(pComentarioId, pUsuarioId);
            }
            catch (Exception ex)
            {
                //Cerramos las conexiones
                ControladorConexiones.CerrarConexiones();

                //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                while (!pVirtuosoAD.ServidorOperativo())
                {
                    //Dormimos 30 segundos
                    Thread.Sleep(30 * 1000);
                }

                facCN = new FacetadoCN(mUrlIntragnoss, "", pEntityContext, pLoggingService, mConfigService, pVirtuosoAD, servicesUtilVirtuosoAndReplication);
                comentarioLeido = facCN.ComprobarSiComentarioNoExisteOHaSidoLeidoPerfil(pComentarioId, pUsuarioId);
            }
            finally
            {
                if (facCN != null)
                {
                    facCN.Dispose();
                    facCN = null;
                }
            }

            return comentarioLeido;
        }

        /// <summary>
        /// Pasa una cadena de texto a UTF8.
        /// </summary>
        /// <param name="cadena">Cadena</param>
        /// <returns>cadena de texto en UTF8</returns>
        public static string PasarAUtf8(string cadena)
        {
            Encoding EncodingANSI = Encoding.GetEncoding("iso8859-1");
            return EncodingANSI.GetString(Encoding.UTF8.GetBytes(cadena));
        }

        private void InsertaTripletasConModify_ControlCheckPoint(EntityContext pEntityContext, LoggingService pLoggingService, VirtuosoAD pVirtuosoAD, string pTablaReplica, string pProyectoID, string ptripletas, Dictionary<string, string> pListaElementosaModificarID, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoAD facetadoAD = null;
            try
            {
                facetadoAD = new FacetadoAD("home", mUrlIntragnoss, pTablaReplica, pLoggingService, pEntityContext, mConfigService, pVirtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                facetadoAD.InsertaTripletasConModify(pProyectoID, ptripletas, pListaElementosaModificarID);
            }
            catch (Exception ex)
            {
                //Cerramos las conexiones
                ControladorConexiones.CerrarConexiones();

                //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                while (!pVirtuosoAD.ServidorOperativo())
                {
                    //Dormimos 30 segundos
                    Thread.Sleep(30 * 1000);
                }

                facetadoAD = new FacetadoAD("home", mUrlIntragnoss, pTablaReplica, pLoggingService, pEntityContext, mConfigService, pVirtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;
                facetadoAD.InsertaTripletasConModify(pProyectoID, ptripletas, pListaElementosaModificarID);

            }
            finally
            {
                facetadoAD.Dispose();
                facetadoAD = null;
            }
        }

        private void InsertaTripletas_ControlCheckPoint(EntityContext pEntityContext, LoggingService pLoggingService, VirtuosoAD pVirtuosoAD, string pTablaReplica, string pProyectoID, string ptripletas, short pPrioridad, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoAD facetadoAD = null;
            try
            {
                facetadoAD = new FacetadoAD("home", mUrlIntragnoss, pLoggingService, pEntityContext, mConfigService, pVirtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                facetadoAD.InsertaTripletas(pProyectoID, ptripletas, pPrioridad);
            }
            catch (Exception ex)
            {
                //Cerramos las conexiones
                ControladorConexiones.CerrarConexiones();

                //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                while (!pVirtuosoAD.ServidorOperativo())
                {
                    //Dormimos 30 segundos
                    Thread.Sleep(30 * 1000);
                }

                facetadoAD = new FacetadoAD("home", mUrlIntragnoss, pLoggingService, pEntityContext, mConfigService, pVirtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;
                facetadoAD.InsertaTripletas(pProyectoID, ptripletas, pPrioridad);

            }
            finally
            {
                facetadoAD.Dispose();
                facetadoAD = null;
            }
        }

        #endregion

        #region Utilidades tags

        /// <summary>
        /// Formatea un tag para realizar un select a un DataSet
        /// </summary>
        /// <param name="pTag">Tag</param>
        /// <returns>El tag formateado</returns>
        private string FormatearTagParaSelect(string pTag)
        {
            return "'" + pTag.Replace("'", "\''") + "'";
        }

        /// <summary>
        /// Comprueba si un tag proviene de un filtro
        /// </summary>
        /// <param name="pTags">Cadena que contiene los tags</param>
        /// <param name="pListaTagsFiltros">Lista de tags que provienen de filtros</param>
        /// <param name="pListaTodosTags">Lista de todos los tags</param>
        /// <param name="pDataSet">Data set de la fila de cola</param>
        /// <returns></returns>
        private Dictionary<short, List<string>> ObtenerTagsFiltros(string pTags, DataSet pDataSet)
        {
            Dictionary<short, List<string>> listaTagsFiltros = new Dictionary<short, List<string>>();

            if (pDataSet is BaseMensajesDS)
            {
                //MensajeID
                listaTagsFiltros.Add((short)TiposTags.IDTagMensaje, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_MENSAJE));

                //Identidad que envia el mensaje
                listaTagsFiltros.Add((short)TiposTags.IDTagMensajeFrom, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_MENSAJE_FROM));

                //Identidad que recibe el mensaje
                listaTagsFiltros.Add((short)TiposTags.IDTagMensajeTo, BuscarTagFiltroEnCadena(ref pTags, Constantes.IDS_MENSAJE_TO));
            }
            else if (pDataSet is BaseComentariosDS)
            {
                //ComentarioID
                listaTagsFiltros.Add((short)TiposTags.IDTagComentario, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_COMENTARIO));

                //Identidad que envia el mensaje
                listaTagsFiltros.Add((short)TiposTags.IDsTagComentarioPerfil, BuscarTagFiltroEnCadena(ref pTags, Constantes.IDS_COMENTARIO_PERFIL));

            }
            else if (pDataSet is BaseInvitacionesDS)
            {
                //InvitaciónID
                listaTagsFiltros.Add((short)TiposTags.IDTagInvitacion, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_INVITACION));

                //Identidad del destino de la invitacion
                listaTagsFiltros.Add((short)TiposTags.IDTagInvitacionIdDestino, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_INVITACION_IDDESTINO));

            }
            else if (pDataSet is BaseSuscripcionesDS)
            {
                //SuscripcionID
                listaTagsFiltros.Add((short)TiposTags.IDTagSuscripcion, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_SUSCRIPCION));

                //Recurso asociado a la suscripcion
                listaTagsFiltros.Add((short)TiposTags.IDTagSuscripcionRecurso, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_SUSCRIPCION_RECURSO));

                //Identidad del destino de la suscripcion
                listaTagsFiltros.Add((short)TiposTags.IDTagSuscripcionPerfil, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_SUSCRIPCION_PERFIL));

            }
            else if (pDataSet is BaseContactosDS)
            {
                //Contacto
                listaTagsFiltros.Add((short)TiposTags.IDTagContacto, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_CONTACTO));

                //Usuario que hace un contacto
                listaTagsFiltros.Add((short)TiposTags.IDTagIdentidad, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_IDENTIDAD));
            }
            return listaTagsFiltros;
        }

        /// <summary>
        /// Busca un filtro concreto en una cadena
        /// </summary>
        /// <param name="pCadena">Cadena en la que se debe buscar</param>
        /// <param name="pClaveFiltro">Clave del filtro (##CAT_DOC##, ...)</param>
        /// <returns></returns>
        private List<string> BuscarTagFiltroEnCadena(ref string pCadena, string pClaveFiltro)
        {
            string filtro = "";
            List<string> listaFiltros = new List<string>();

            int indiceFiltro = pCadena.IndexOf(pClaveFiltro);

            if (indiceFiltro >= 0)
            {
                string subCadena = pCadena.Substring(indiceFiltro + pClaveFiltro.Length);

                filtro = subCadena.Substring(0, subCadena.IndexOf(pClaveFiltro));

                if ((pClaveFiltro.Equals(Constantes.TIPO_DOC)) || (pClaveFiltro.Equals(Constantes.PERS_U_ORG)) || (pClaveFiltro.Equals(Constantes.ESTADO_COMENTADO)))
                {
                    //Estos tags van con la clave del tag (para tags de tipo entero o similar, ej: Tipos de documento, para que al buscar '0' no aparezcan los tags de todos los recursos que son de tal tipo). 
                    filtro = pClaveFiltro + filtro + pClaveFiltro;
                    pCadena = pCadena.Replace(filtro, "");
                }
                else
                {
                    pCadena = pCadena.Replace(pClaveFiltro + filtro + pClaveFiltro, "");
                    filtro = filtro.ToLower();
                }
                if (filtro.Trim() != "")
                {
                    listaFiltros.Add(filtro);
                }
                listaFiltros.AddRange(BuscarTagFiltroEnCadena(ref pCadena, pClaveFiltro));
            }
            return listaFiltros;
        }

        #endregion

        #region Actualización de la BD

        

        #endregion

        #endregion

        #endregion

        #region Métodos sobreescritos

        protected override ControladorServicioGnoss ClonarControlador()
        {
            SocialSearchController controlador = new SocialSearchController(ScopedFactory, mConfigService, mReplicacion, mUrlServicioEtiquetas);
            return controlador;
        }

        public override void CancelarTarea()
        {
            if (mClienteRabbitMensajes != null)
            {
                mClienteRabbitMensajes.CerrarConexionLectura();
            }
            if (mClienteRabbitComentarios != null)
            {
                mClienteRabbitComentarios.CerrarConexionLectura();
            }
            if (mClienteRabbitSuscripciones != null)
            {
                mClienteRabbitSuscripciones.CerrarConexionLectura();
            }
        }

        #endregion
    }
}
