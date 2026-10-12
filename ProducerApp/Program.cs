using System;
using System.Threading.Tasks;
using Confluent.Kafka;

// Konfigürasyonları ortam değişkenlerinden (Environment Variables) alalım, yoksa yerel varsayılanları kullanalım.
// Bu sayede hassas şifreler, bağlantı adresleri ve kimlik bilgileri kaynak kodda veya Git geçmişinde açıkta kalmaz.
string bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") 
    ?? "localhost:9092,localhost:9094,localhost:9096";
string topicName = Environment.GetEnvironmentVariable("KAFKA_TOPIC") 
    ?? "siparis-olaylari";

string? saslUsername = Environment.GetEnvironmentVariable("KAFKA_SASL_USERNAME");
string? saslPassword = Environment.GetEnvironmentVariable("KAFKA_SASL_PASSWORD");
string? securityProtocolStr = Environment.GetEnvironmentVariable("KAFKA_SECURITY_PROTOCOL");
string? saslMechanismStr = Environment.GetEnvironmentVariable("KAFKA_SASL_MECHANISM");

var config = new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    Acks = Acks.All, // Tüm replikaların onayını bekle (Veri kaybını önlemek için)
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

// String key ve string value kullanıyoruz: <string, string>
using var producer = new ProducerBuilder<string, string>(config).Build();

Console.WriteLine("=== SENARYO: KEY-BASED PRODUCER ===");
Console.WriteLine($"Brokerlar : {config.BootstrapServers}");
Console.WriteLine($"Topic     : {topicName}");
Console.WriteLine("Format    : musteri_id:mesaj (Örnek: musteri-1:Karttan çekildi)");
Console.WriteLine("            (Key boş bırakılırsa round-robin dağıtılır, örn: :Genel Bildirim)");
Console.WriteLine("Çıkış için: q\n");

while (true)
{
    Console.Write("Girdi (Key:Value) > ");
    string? input = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(input) || input.Trim().ToLower() == "q")
        break;

    // "Key:Value" formatını ayırıyoruz (Değerin içinde iki nokta üst üste olabileceği için 2 parçaya bölüyoruz)
    var parts = input.Split(':', 2);
    if (parts.Length < 2)
    {
        Console.WriteLine("[UYARI]: Lütfen 'Anahtar:Değer' formatında girin. (Örnek: musteri-1:Sipariş Alındı)\n");
        continue;
    }

    string key = parts[0].Trim();
    string value = parts[1].Trim();

    // Eğer key boşsa null gönderilir (round-robin / sticky partitioner), doluysa key ile hash'lenir (aynı müşterinin mesajları aynı partition'a gider)
    string? messageKey = string.IsNullOrEmpty(key) ? null : key;

    try
    {
        var deliveryReport = await producer.ProduceAsync(
            topicName,
            new Message<string, string> { Key = messageKey!, Value = value }
        );

        Console.WriteLine($"-> Gönderildi | Key: {messageKey ?? "(null)"} | Partition: {deliveryReport.Partition.Value} | Offset: {deliveryReport.Offset.Value}\n");
    }
    catch (ProduceException<string, string> ex)
    {
        Console.WriteLine($"[HATA]: {ex.Error.Reason}\n");
    }
}

