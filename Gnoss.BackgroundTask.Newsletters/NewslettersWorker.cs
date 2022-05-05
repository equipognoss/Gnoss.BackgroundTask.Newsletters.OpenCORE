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
        private readonly ILogger<NewslettersWorker> _logger;
        private readonly ConfigService _configService;

        public NewslettersWorker(ILogger<NewslettersWorker> logger, ConfigService configService, IServiceScopeFactory scopeFactory)
            : base(logger, scopeFactory)
        {
            _logger = logger;
            _configService = configService;
        }

        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            controladores.Add(new Controller(ScopedFactory, _configService));
            return controladores;
        }
    }
}
