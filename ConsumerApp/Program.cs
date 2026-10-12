using System;
using System.Threading;
using Confluent.Kafka;

// Konfigürasyonları ortam değişkenlerinden (Environment Variables) alalım, yoksa yerel varsayılanları kullanalım.
// Bu sayede hassas şifreler, bağlantı adresleri ve kimlik bilgileri kaynak kodda veya Git geçmişinde açıkta kalmaz.
string bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") 
    ?? "localhost:9092,localhost:9094,localhost:9096";
string topicName = Environment.GetEnvironmentVariable("KAFKA_TOPIC") 
    ?? "siparis-olaylari";
string groupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") 
    ?? "siparis-takip-servisi";

string? saslUsername = Environment.GetEnvironmentVariable("KAFKA_SASL_USERNAME");
string? saslPassword = Environment.GetEnvironmentVariable("KAFKA_SASL_PASSWORD");
string? securityProtocolStr = Environment.GetEnvironmentVariable("KAFKA_SECURITY_PROTOCOL");
string? saslMechanismStr = Environment.GetEnvironmentVariable("KAFKA_SASL_MECHANISM");

// Her çalışan Consumer örneği için rastgele kısa bir kimlik üretelim
string consumerId = "C-" + Guid.NewGuid().ToString()[..4];

var config = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = groupId, // GroupId AYNI kalmalı (Consumer Group yük dengelemesi için)
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

// Eğer SASL kimlik doğrulama bilgileri tanımlanmışsa ayarlara ekle (Confluent Cloud, Upstash vb.)
if (!string.IsNullOrWhiteSpace(saslUsername) && !string.IsNullOrWhiteSpace(saslPassword))
{
    config.SaslUsername = saslUsername;
    config.SaslPassword = saslPassword;
    config.SecurityProtocol = Enum.TryParse<SecurityProtocol>(securityProtocolStr, true, out var sp) 
        ? sp 
        : SecurityProtocol.SaslSsl;
    config.SaslMechanism = Enum.TryParse<SaslMechanism>(saslMechanismStr, true, out var sm) 
        ? sm 
        : SaslMechanism.Plain;
}

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe(topicName);

Console.WriteLine($"=== [{consumerId}] CONSUMER DİNLENİYOR ===");
Console.WriteLine($"Brokerlar : {config.BootstrapServers}");
Console.WriteLine($"Grup ID   : {config.GroupId}");
Console.WriteLine($"Topic     : {topicName}");
Console.WriteLine("Mesajlar bekleniyor... (Durdurmak için Ctrl+C)\n");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        try
        {
            var consumeResult = consumer.Consume(cts.Token);
            string keyDisplay = consumeResult.Message.Key ?? "(null)";
            Console.WriteLine($"[{consumerId}] -> Partition: {consumeResult.Partition.Value} | Key: {keyDisplay,-12} | Mesaj: {consumeResult.Message.Value}");
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"[HATA]: {ex.Error.Reason}");
        }
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine($"\n[{consumerId}] Consumer kapatılıyor, offsetler kaydedildi.");
    consumer.Close();
}

