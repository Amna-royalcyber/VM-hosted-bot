using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Calls.Media;
using Microsoft.Graph.Communications.Client;
using Microsoft.Graph.Communications.Resources;
using Microsoft.Graph.Models;

namespace TeamsMediaBot;

public sealed class CallHandler
{
    private readonly BotSettings _settings;
    private readonly ILogger<CallHandler> _logger;
    private ICommunicationsClient? _communicationsClient;

    public CallHandler(BotSettings settings, ILogger<CallHandler> logger)
    {
        _settings = settings;
        _logger = logger;
    }

    public void Initialize(ICommunicationsClient communicationsClient)
    {
        _communicationsClient = communicationsClient;
        _communicationsClient.Calls().OnIncoming += OnIncomingCall;
    }

    public async Task<ICall> JoinMeetingByUrlAsync(string joinUrl, MediaHandler mediaHandler)
    {
        if (_communicationsClient is null)
        {
            throw new InvalidOperationException("Communications client has not been initialized.");
        }

        IMediaSession mediaSession = mediaHandler.CreateMediaSession(_communicationsClient);
        var (chatInfo, meetingInfo) = CreateJoinInfoFromUrl(joinUrl);

        var joinMeetingParameters = new JoinMeetingParameters(
            chatInfo,
            meetingInfo,
            mediaSession,
            null,
            null,
            false)
        {
            TenantId = _settings.TenantId
        };

        ICall call = await _communicationsClient
            .Calls()
            .AddAsync(joinMeetingParameters);

        call.OnUpdated += (_, args) =>
        {
            _logger.LogInformation("Call state updated. State: {State}", args.NewResource?.State);
        };

        _logger.LogInformation("Join request submitted. Call ID: {CallId}", call.Id);
        return call;
    }

    private void OnIncomingCall(ICallCollection _, CollectionEventArgs<ICall> args)
    {
        foreach (ICall incomingCall in args.AddedResources)
        {
            _logger.LogInformation("Incoming call received. Call ID: {CallId}", incomingCall.Id);
        }
    }

    private static (ChatInfo ChatInfo, MeetingInfo MeetingInfo) CreateJoinInfoFromUrl(string joinUrl)
    {
        var encoded = Uri.EscapeDataString(joinUrl);
        var chatInfo = new ChatInfo
        {
            ThreadId = encoded,
            MessageId = "0"
        };

        var meetingInfo = new OrganizerMeetingInfo
        {
            Organizer = new IdentitySet
            {
                User = new Identity
                {
                    Id = "00000000-0000-0000-0000-000000000000"
                }
            }
        };

        meetingInfo.AdditionalData = new Dictionary<string, object>
        {
            ["joinWebUrl"] = joinUrl
        };

        return (chatInfo, meetingInfo);
    }
}
