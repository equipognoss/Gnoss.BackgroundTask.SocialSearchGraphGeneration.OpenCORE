using Es.Riam.Gnoss.Elementos.Suscripcion;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.Util.General;
using GnossServicioModuloBaseUsuarios;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gnoss.BackgroundTask.SocialSearchGraphGeneration
{
    public class SocialSearchGraphGenerationWorker : Worker
    {
        private readonly ConfigService _configService;
        private ILogger mlogger;
        private ILoggerFactory mLoggerFactory;

        public SocialSearchGraphGenerationWorker(ConfigService configService, IServiceScopeFactory scopeFactory, ILogger<SocialSearchGraphGenerationWorker> logger, ILoggerFactory loggerFactory) : base(logger, scopeFactory)
        {
            _configService = configService;
            mlogger = logger;
            mLoggerFactory = loggerFactory;
        }

        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            ControladorServicioGnoss.INTERVALO_SEGUNDOS = _configService.ObtenerIntervalo();
            bool replicacion = false;

            if (_configService.ObtenerReplicacionActivada())
            {
                replicacion = true;
            }

            string urlServicioEtiquetas = "";
            if (!string.IsNullOrEmpty(_configService.ObtenerUrlServicioEtiquetas()))
            {
                urlServicioEtiquetas = _configService.ObtenerUrlServicioEtiquetas();
            }

            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            controladores.Add(new SocialSearchController(ScopedFactory, _configService, replicacion, urlServicioEtiquetas, mLoggerFactory.CreateLogger<SocialSearchController>(), mLoggerFactory));

            return controladores;
        }
    }
}
