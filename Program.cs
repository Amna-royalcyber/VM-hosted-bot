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
            AwsRegion = GetConfig(builder.Configuration, "AWS_REGION", "AWS:Region")
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
            catch (Exception ex)
            {
                log.LogError(ex, "Join meeting failed.");
                return Results.Json(
                    new { message = "Join meeting failed.", error = ex.Message },
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
}
