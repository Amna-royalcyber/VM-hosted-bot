using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Client;
using Microsoft.Graph.Communications.Client.Authentication;
using Microsoft.Graph.Communications.Common.Telemetry;
using System.Net.Http.Headers;

namespace TeamsMediaBot;

public sealed class BotSettings
{
    public required string TenantId { get; init; }
    public required string ClientId { get; init; }
    public required string ClientSecret { get; init; }
    public required string ApplicationName { get; init; }
    public required string ServiceBaseUrl { get; init; }
    public required string AwsRegion { get; init; }
}

public sealed class BotService
{
    private readonly BotSettings _settings;
    private readonly CallHandler _callHandler;
    private readonly MediaHandler _mediaHandler;
    private readonly AwsTranscribeService _awsTranscribeService;
    private readonly ILogger<BotService> _logger;
    private readonly IGraphLogger _graphLogger;
    private ICommunicationsClient? _communicationsClient;
    private bool _isInitialized;

    public BotService(
        BotSettings settings,
        CallHandler callHandler,
        MediaHandler mediaHandler,
        AwsTranscribeService awsTranscribeService,
        ILogger<BotService> logger)
    {
        _settings = settings;
        _callHandler = callHandler;
        _mediaHandler = mediaHandler;
        _awsTranscribeService = awsTranscribeService;
        _logger = logger;
        _graphLogger = new GraphLogger(_settings.ApplicationName);
    }

    public async Task JoinMeetingAsync(string meetingJoinUrl)
    {
        EnsureInitialized();
        if (_communicationsClient is null)
        {
            throw new InvalidOperationException("Communications client is not initialized.");
        }

        _logger.LogInformation("Joining Teams meeting from join URL.");
        await _awsTranscribeService.StartStreaming();
        await _callHandler.JoinMeetingByUrlAsync(meetingJoinUrl, _mediaHandler);
        _logger.LogInformation("Join request submitted to Graph.");
    }

    private void EnsureInitialized()
    {
        if (_isInitialized)
        {
            return;
        }

        _communicationsClient = CreateCommunicationsClient();
        _callHandler.Initialize(_communicationsClient);
        _isInitialized = true;
    }

    private ICommunicationsClient CreateCommunicationsClient()
    {
        var credential = new ClientSecretCredential(_settings.TenantId, _settings.ClientId, _settings.ClientSecret);
        var authProvider = new ClientCredentialsAuthenticationProvider(credential, _settings.TenantId);

        // SDK requires BOTH: service base (origin) and notification (callback) URLs.
        // Bot:CallbackUrl / BOT_SERVICE_BASE_URL should be the full HTTPS callback, e.g. https://host/callback
        var notificationUrl = _settings.ServiceBaseUrl.Trim();
        if (string.IsNullOrWhiteSpace(notificationUrl))
        {
            throw new InvalidOperationException(
                "Callback URL is empty. Set Bot:CallbackUrl or BOT_SERVICE_BASE_URL to your public HTTPS callback (e.g. https://bot.example.com/callback).");
        }

        var notificationUri = new Uri(notificationUrl, UriKind.Absolute);
        if (notificationUri.Scheme != Uri.UriSchemeHttps)
        {
            throw new InvalidOperationException("Callback URL must use HTTPS.");
        }

        var authority = notificationUri.GetLeftPart(UriPartial.Authority);
        var serviceBaseUri = new Uri(authority + "/", UriKind.Absolute);

        return new CommunicationsClientBuilder(
                _settings.ClientId,
                _settings.ApplicationName,
                _graphLogger)
            .SetAuthenticationProvider(authProvider)
            .SetServiceBaseUrl(serviceBaseUri)
            .SetNotificationUrl(notificationUri)
            .Build();
    }
}

public sealed class ClientCredentialsAuthenticationProvider : IRequestAuthenticationProvider
{
    private static readonly TokenRequestContext GraphScope = new(new[] { "https://graph.microsoft.com/.default" });
    private readonly TokenCredential _credential;
    private readonly string _tenantId;

    public ClientCredentialsAuthenticationProvider(TokenCredential credential, string tenantId)
    {
        _credential = credential;
        _tenantId = tenantId;
    }

    public async Task AuthenticateOutboundRequestAsync(HttpRequestMessage request, string tenant)
    {
        AccessToken token = await _credential.GetTokenAsync(GraphScope, default);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);
    }

    public Task<RequestValidationResult> ValidateInboundRequestAsync(HttpRequestMessage request)
    {
        // Demo-only: validate inbound token/signature for production webhooks.
        return Task.FromResult(new RequestValidationResult
        {
            IsValid = true,
            TenantId = _tenantId
        });
    }
}
