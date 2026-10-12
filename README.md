# Apache Kafka .NET 9 Demo

Bu proje, **.NET 9** ve **Confluent.Kafka** kullanılarak geliştirilmiş sipariş takip (`siparis-olaylari`) senaryosuna sahip bir Kafka demo uygulamasıdır.

---

## 📦 Proje Bileşenleri

- **`ProducerApp` (Üretici):** Konsoldan girilen sipariş olaylarını Kafka'ya iletir.
  - Girdi Formatı: `musteri_id:mesaj` (Örn: `musteri-1:Siparis Alindi`)
  - **Key-Based Gönderim:** Mesajlar müşteri ID'sine (key) göre ilgili partition'a yönlendirilir; böylece aynı müşterinin sipariş adımları sırayla işlenir.
- **`ConsumerApp` (Tüketici):** `siparis-olaylari` topic'ini dinler.
  - Tüketici Grubu: `siparis-takip-servisi`
  - Birden fazla kopya çalıştırıldığında partition'lar tüketiciler arasında otomatik olarak paylaştırılır.
- **`docker-compose.yml`:** Yerel ortamda çalıştırmak için 3 broker'lı Kafka kümesi ve [Kafdrop](http://localhost:9000) web arayüzünü içerir.

---

## 🚀 Çalıştırma Adımları

### 1. Kafka Kümesini Başlatın
```bash
docker compose up -d
```
*(Broker adresleri: `localhost:9092, localhost:9094, localhost:9096` | Web Arayüzü: `http://localhost:9000`)*

### 2. Consumer Uygulamasını Çalıştırın
```bash
dotnet run --project ConsumerApp
```
*(Yük dengelemesini gözlemlemek için birden fazla terminalde çalıştırabilirsiniz.)*

### 3. Producer Uygulamasını Çalıştırın
```bash
dotnet run --project ProducerApp
```
Konsoldan mesaj girin:
```text
musteri-1:Siparis Olusturuldu
musteri-1:Odeme Yapildi
musteri-2:Siparis Olusturuldu
```

