using Microsoft.AspNetCore.Builder;
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
        app.UseDefaultFiles();
        app.UseStaticFiles();

        app.MapHub<TranscriptHub>("/hubs/transcripts");
        app.MapPost("/api/bot/join", async (JoinMeetingRequest request, BotService botService) =>
        {
            if (string.IsNullOrWhiteSpace(request.MeetingJoinUrl))
            {
                return Results.BadRequest(new { message = "MeetingJoinUrl is required." });
            }

            await botService.JoinMeetingAsync(request.MeetingJoinUrl);
            return Results.Ok(new { message = "Join request submitted." });
        });

        await app.RunAsync();
    }

    private static string GetConfig(IConfiguration configuration, string envKey, string configKey)
    {
        var value = Environment.GetEnvironmentVariable(envKey) ?? configuration[configKey];
        return value ?? throw new InvalidOperationException($"Missing configuration: {envKey} or {configKey}");
    }
}
