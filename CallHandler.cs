using System.Text.RegularExpressions;
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
        var (chatInfo, meetingInfo) = CreateJoinInfoFromUrl(joinUrl, _logger);

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

    /// <summary>
    /// Parses a Teams meeting join URL (meetup-join) into ChatInfo / MeetingInfo.
    /// ThreadId must be the thread segment (e.g. 19:meeting_...@thread.v2), not the full URL — otherwise Graph returns 404 NotFound.
    /// </summary>
    private static (ChatInfo ChatInfo, MeetingInfo MeetingInfo) CreateJoinInfoFromUrl(string joinUrl, ILogger logger)
    {
        if (string.IsNullOrWhiteSpace(joinUrl))
        {
            throw new ArgumentException("Join URL is empty.", nameof(joinUrl));
        }

        var normalized = NormalizeTeamsJoinUrl(joinUrl.Trim(), logger);
        var uri = new Uri(normalized);

        if (!TryExtractMeetupJoinThreadAndMessage(uri, out var threadId, out var messageId))
        {
            throw new ArgumentException(
                "Could not parse this Teams link. Copy the full join link from the meeting invite (Open in Teams → Copy join link). " +
                "It must contain meetup-join and look like: https://teams.microsoft.com/l/meetup-join/19%3A.../0?... " +
                "Launcher links (launcher.html?url=...) are supported after normalization.",
                nameof(joinUrl));
        }

        var chatInfo = new ChatInfo
        {
            ThreadId = threadId,
            MessageId = messageId
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
            ["joinWebUrl"] = normalized
        };

        return (chatInfo, meetingInfo);
    }

    /// <summary>
    /// Unwraps Teams launcher URLs so the path contains /meetup-join/...
    /// </summary>
    private static string NormalizeTeamsJoinUrl(string joinUrl, ILogger logger)
    {
        if (!Uri.TryCreate(joinUrl, UriKind.Absolute, out var uri))
        {
            throw new ArgumentException("Join URL must be a valid absolute https URL.", nameof(joinUrl));
        }

        if (uri.Scheme != Uri.UriSchemeHttps)
        {
            throw new ArgumentException("Join URL must use https.", nameof(joinUrl));
        }

        var current = joinUrl;
        for (var i = 0; i < 3; i++)
        {
            if (!Uri.TryCreate(current, UriKind.Absolute, out uri))
            {
                break;
            }

            if (!uri.AbsolutePath.Contains("launcher", StringComparison.OrdinalIgnoreCase))
            {
                break;
            }

            var inner = GetQueryParameter(uri.Query, "url");
            if (string.IsNullOrEmpty(inner))
            {
                break;
            }

            inner = Uri.UnescapeDataString(inner);
            if (inner.StartsWith('/'))
            {
                current = $"{uri.Scheme}://{uri.Host}{inner}";
            }
            else if (inner.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
                     inner.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            {
                current = inner.Replace("http://", "https://", StringComparison.OrdinalIgnoreCase);
            }
            else
            {
                current = $"{uri.Scheme}://{uri.Host}/{inner.TrimStart('/')}";
            }

            logger.LogInformation("Unwrapped Teams launcher URL to meeting path.");
        }

        return current;
    }

    private static string? GetQueryParameter(string query, string key)
    {
        if (string.IsNullOrEmpty(query) || query[0] == '?')
        {
            query = query.TrimStart('?');
        }

        foreach (var part in query.Split('&'))
        {
            var eq = part.IndexOf('=');
            if (eq <= 0)
            {
                continue;
            }

            var name = part[..eq];
            if (!name.Equals(key, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            return part[(eq + 1)..];
        }

        return null;
    }

    private static bool TryExtractMeetupJoinThreadAndMessage(Uri uri, out string threadId, out string messageId)
    {
        threadId = null!;
        messageId = null!;

        var match = MeetupJoinRegex.Match(uri.AbsolutePath);
        if (match.Success)
        {
            threadId = Uri.UnescapeDataString(match.Groups[1].Value);
            messageId = Uri.UnescapeDataString(match.Groups[2].Value);
            return true;
        }

        var segments = uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var meetupIdx = Array.FindIndex(
            segments,
            s => s.Equals("meetup-join", StringComparison.OrdinalIgnoreCase));

        if (meetupIdx < 0 || meetupIdx + 2 >= segments.Length)
        {
            return false;
        }

        threadId = Uri.UnescapeDataString(segments[meetupIdx + 1]);
        messageId = Uri.UnescapeDataString(segments[meetupIdx + 2]);
        return true;
    }

    private static readonly Regex MeetupJoinRegex = new(
        @"meetup-join/([^/?#]+)/([^/?#]+)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
}
