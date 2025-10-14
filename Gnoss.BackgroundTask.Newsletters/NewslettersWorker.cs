using Es.Riam.Gnoss.Elementos.Suscripcion;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.Win.ServicioEnviosMasivos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gnoss.BackgroundTask.Newsletters
{
    public class NewslettersWorker : Worker
    {
        private readonly ConfigService _configService;
        private ILogger mlogger;
        private ILoggerFactory mLoggerFactory;

        public NewslettersWorker(ConfigService configService, IServiceScopeFactory scopeFactory, ILogger<NewslettersWorker> logger, ILoggerFactory loggerFactory)
            : base(logger, scopeFactory)
        {
            _configService = configService;
            mlogger = logger;
            mLoggerFactory = loggerFactory;
        }

        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            controladores.Add(new Controller(ScopedFactory, _configService, mLoggerFactory.CreateLogger<Controller>(), mLoggerFactory));
            return controladores;
        }
    }
}
