using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Logging.ClearProviders();
        builder.Logging.AddConsole();

        builder.Services.Configure<ForwardedHeadersOptions>(options =>
        {
            options.ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
            options.KnownNetworks.Clear();
            options.KnownProxies.Clear();
        });

        builder.Services.AddSignalR();
        builder.Services.AddSingleton(new BotSettings
        {
            TenantId = GetConfig(builder.Configuration, "BOT_TENANT_ID", "AzureAd:TenantId"),
            ClientId = GetConfig(builder.Configuration, "BOT_CLIENT_ID", "AzureAd:ClientId"),
            ClientSecret = GetConfig(builder.Configuration, "BOT_CLIENT_SECRET", "AzureAd:ClientSecret"),
            ApplicationName = Environment.GetEnvironmentVariable("BOT_APP_NAME") ?? "TeamsMediaBot",
            ServiceBaseUrl = GetConfig(builder.Configuration, "BOT_SERVICE_BASE_URL", "Bot:CallbackUrl"),
            AwsRegion = GetConfig(builder.Configuration, "AWS_REGION", "AWS:Region"),
            MediaCertificateThumbprint = GetConfig(builder.Configuration, "BOT_MEDIA_CERT_THUMBPRINT", "Media:CertificateThumbprint"),
            MediaPublicIp = GetConfig(builder.Configuration, "BOT_MEDIA_PUBLIC_IP", "Media:PublicIp"),
            MediaInstanceInternalPort = ReadInt(builder.Configuration, "BOT_MEDIA_INSTANCE_INTERNAL_PORT", "Media:InstanceInternalPort", 8445),
            MediaInstancePublicPort = ReadInt(builder.Configuration, "BOT_MEDIA_INSTANCE_PUBLIC_PORT", "Media:InstancePublicPort", 8445),
            MediaServiceFqdn = ReadOptional(builder.Configuration, "BOT_MEDIA_SERVICE_FQDN", "Media:ServiceFqdn")
        });

        builder.Services.AddSingleton<TranscriptBroadcaster>();
        builder.Services.AddSingleton<AudioProcessor>();
        builder.Services.AddSingleton<AwsTranscribeService>();
        builder.Services.AddSingleton<MediaHandler>();
        builder.Services.AddSingleton<CallHandler>();
        builder.Services.AddSingleton<BotService>();

        var app = builder.Build();

        // Must run first so SignalR and HTTPS URLs see correct scheme/host behind nginx.
        app.UseForwardedHeaders();

        app.UseDefaultFiles();
        app.UseStaticFiles();

        app.MapHub<TranscriptHub>("/hubs/transcripts");
        app.MapPost("/api/bot/join", async (JoinMeetingRequest request, BotService botService, ILoggerFactory loggerFactory) =>
        {
            var log = loggerFactory.CreateLogger("TeamsMediaBot.Join");

            if (string.IsNullOrWhiteSpace(request.MeetingJoinUrl))
            {
                return Results.BadRequest(new { message = "MeetingJoinUrl is required." });
            }

            try
            {
                await botService.JoinMeetingAsync(request.MeetingJoinUrl);
                return Results.Ok(new { message = "Join request submitted." });
            }
            catch (ArgumentException ex)
            {
                log.LogWarning(ex, "Invalid join URL.");
                return Results.BadRequest(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Join meeting failed.");
                return Results.Json(
                    new
                    {
                        message = "Join meeting failed.",
                        error = ex.Message,
                        inner = ex.InnerException?.Message,
                        type = ex.GetType().FullName
                    },
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        });

        await app.RunAsync();
    }

    private static string GetConfig(IConfiguration configuration, string envKey, string configKey)
    {
        var env = Environment.GetEnvironmentVariable(envKey);
        if (!string.IsNullOrWhiteSpace(env))
        {
            return env.Trim();
        }

        var fromConfig = configuration[configKey];
        if (!string.IsNullOrWhiteSpace(fromConfig))
        {
            return fromConfig.Trim();
        }

        throw new InvalidOperationException($"Missing configuration: {envKey} or {configKey}");
    }

    private static string? ReadOptional(IConfiguration configuration, string envKey, string configKey)
    {
        var env = Environment.GetEnvironmentVariable(envKey);
        if (!string.IsNullOrWhiteSpace(env))
        {
            return env.Trim();
        }

        var fromConfig = configuration[configKey];
        return string.IsNullOrWhiteSpace(fromConfig) ? null : fromConfig.Trim();
    }

    private static int ReadInt(IConfiguration configuration, string envKey, string configKey, int defaultValue)
    {
        var env = Environment.GetEnvironmentVariable(envKey);
        if (!string.IsNullOrWhiteSpace(env) && int.TryParse(env.Trim(), out var fromEnv))
        {
            return fromEnv;
        }

        var fromConfig = configuration[configKey];
        if (!string.IsNullOrWhiteSpace(fromConfig) && int.TryParse(fromConfig.Trim(), out var fromFile))
        {
            return fromFile;
        }

        return defaultValue;
    }
}
