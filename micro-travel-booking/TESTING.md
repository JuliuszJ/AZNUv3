# Test przeplywu rezerwacji

Uruchomienie w PowerShell:

```powershell
mvn '-P!jboss-public-repository' '-Dtest=LegacyBookingFlowTests' test -B -ntp
```

Opcja `-P!jboss-public-repository` wylacza dodatkowy profil lokalnych ustawien
Mavena. Jezeli go nie uzywasz, wystarczy `mvn test`.

`LegacyBookingFlowTests` uruchamia rzeczywiste trasy z
`MicroTravelBookingService`, `PaymentService` oraz dwie niezalezne instancje
`StateService`. Kazdy scenariusz otrzymuje nowy kontekst i nowy stan.

Osiemnascie scenariuszy obejmuje:

- wygenerowanie UUID, publikacje pierwotnego requestu z naglowkiem
  `bookingTravelId` i zgodnosc identyfikatora z odpowiedzia `BookingInfo`;
- odpowiedz HTTP 202 ze statusem PENDING i zapis BookingInfo w mapie,
  z zachowaniem osobnych wpisow dla kolejnych rezerwacji;
- sumowanie kosztow USA/JFK do 1699 oraz Poland/WAW do 1488;
- publikacje wyniku na TravelPaymentResultTopic oraz aktualizacje kosztu i
  statusu DONE w mapie gateway po odebraniu wiadomosci;
- zachowanie wyniku DONE, gdy zapis poczatkowego PENDING nastapi pozniej;
- przekazanie CANCELED do gateway oraz zachowanie anulowania pomimo
  duplikatow i spoznionych wiadomosci DONE lub poczatkowego PENDING;
- odczyt zapisanej rezerwacji w stanie PENDING, DONE i CANCELED oraz
  odpowiedz 404 z bledem JSON dla nieznanego identyfikatora;
- rozpoczecie finalizacji dopiero po odebraniu requestu oraz obu rezerwacji,
  zarowno gdy request przychodzi pierwszy, jak i ostatni;
- blad hotelu dla China oraz lotu z PEK: tresc bledu, typ uslugi i identyfikator;
- kompensacje drugiej rezerwacji po jej zakonczeniu;
- anulowanie przed rozpoczeciem drugiej rezerwacji: brak publikacji sukcesu
  i brak finalizacji platnosci.
- dwa testy diagnostyczne dokumentuja obecny blad: payment zachowuje dane
  po anulowaniu, a spozniony sukces brakujacej rezerwacji uruchamia finalizacje.

Kafka i `stream:out` sa zastepowane endpointami `mock:`, a wejscia konsumentow
Kafki endpointami `direct:` przez AdviceWith. Test jawnie przekazuje kopie
wiadomosci pomiedzy konsumentami, symulujac dostarczenie przez topic.
Sprawdzane sa wywolania tras kompensacyjnych; same akcje kompensacyjne
w kodzie produkcyjnym nadal tylko loguja dane.

Test nie uruchamia serwera HTTP ani brokera Kafka. Nie sprawdza grup
konsumentow, partycjonowania, serializacji naglowkow przez broker, retry
ani rownoleglego przetwarzania. Wynik pozytywny potwierdza logike tras
w sprawdzanych kolejnosciach zdarzen, nie caly deployment aplikacji.

Raport: `target/surefire-reports/org.bp.travel.LegacyBookingFlowTests.txt`.

## Test integracyjny REST

```powershell
mvn '-P!jboss-public-repository' '-Dtest=BookingRestIntegrationTests' test -B -ntp
```

`BookingRestIntegrationTests` uruchamia aplikacje Spring Boot z rzeczywistym
serwerem HTTP na losowym porcie. Kafka oraz wyjscie konsoli sa zastepowane
endpointami testowymi przed startem Camel; definicje REST pozostaja aktywne.

Dwa scenariusze sprawdzaja:

- POST /travel/booking -> 202 z BookingInfo PENDING i UUID, a nastepnie
  GET /travel/booking/{id} -> 200 z ta sama rezerwacja; dodatkowo po podaniu
  wyniku payment do wejscia gateway kolejny GET zwraca DONE z kosztem 1699;
- GET nieznanego id -> 404 z JSON-em ExceptionResponse i komunikatem bledu.

Test sprawdza statusy HTTP, Content-Type, serializacje JSON i przekazanie
parametru id. Nie wymaga dzialajacego brokera ani wolnego portu 8090.
Testy bez brokera: `mvn test` (18 scenariuszy tras i 2 scenariusze HTTP).

## Saga z rzeczywistym brokerem Kafka

Broker musi dzialac pod `localhost:9092`. Uruchomienie w PowerShell:

```powershell
mvn '-P!jboss-public-repository' '-Dsaga.kafka=true' '-Dtest=SagaKafkaIntegrationTests' test -B -ntp
```

Bez `-Dsaga.kafka=true` klasa jest pomijana, wiec zwykle `mvn test` nie
wymaga brokera. Test nie uruchamia ani nie zatrzymuje kontenera Docker.

Cztery scenariusze wykonuja POST i GET przez rzeczywisty HTTP oraz przesylaja
wiadomosci przez rzeczywista Kafke:

- USA/JFK: status DONE i koszt 1699 odczytany przez GET;
- China/JFK: blad hotelu, wywolanie kompensacji lotu i GET ze statusem CANCELED;
- USA/PEK: blad lotu, wywolanie kompensacji hotelu i GET ze statusem CANCELED;
- China/PEK: bledy obu uslug i GET ze statusem CANCELED bez kosztu.

Kazde uruchomienie tworzy cztery osobne topiki z prefiksem `saga-it-<UUID>-`.
Konsumenci maja osobne grupy i autoOffsetReset=earliest, aby asynchroniczny
start konsumentow nie zgubil pierwszych wiadomosci. Produkcyjne nazwy
topicow i konfiguracja tras nie sa zmieniane. Po testach kontekst aplikacji
jest zatrzymywany, a topiki testowe usuwane.

Tylko `stream:out` jest zastepowane obserwacja testowa. Potwierdza to
wywolanie tras kompensacyjnych, ktore obecnie nie wykonuja rzeczywistego
anulowania rezerwacji u zewnetrznych dostawcow.

Po bledzie hotelu lub lotu handler publikuje BookingInfo ze statusem CANCELED
na TravelPaymentResultTopic. Niezaleznie wysyla blad na TravelBookingFailTopic,
co uruchamia kompensacje drugiej uslugi. Gateway zapisuje CANCELED bez kosztu;
status oznacza decyzje o anulowaniu, nie potwierdzenie zakonczenia wszystkich
akcji kompensacyjnych. Anulowanie nie moze byc nadpisane spoznionym DONE.

Pelny zestaw: `mvn '-P!jboss-public-repository' '-Dsaga.kafka=true' test -B -ntp`
(24 scenariusze).

## Wykryty brak anulowania w payment

Test `paymentCurrentlyRetainsCanceledSagaAndFinalizesOnLateSuccess` jest
testem charakteryzujacym obecne niepozadane zachowanie, a nie potwierdzeniem
poprawnosci biznesowej. Po bledzie hotelu lub lotu i kompensacji drugiego
kroku payment zachowuje request oraz czesciowe wyniki. Nie odbiera anulowania
i nie ma terminalnego stanu anulowania ani usuwania wpisu.

Po podaniu spoznionego sukcesu brakujacej rezerwacji test wykazuje wywolanie
notification i wygenerowanie DONE przez payment. Gateway nadal zachowuje
CANCELED dzieki osobnemu zabezpieczeniu magazynu. Testy z rzeczywista Kafka
potwierdzaja takze zachowanie niekompletnego requestu w payment po anulowaniu.

Raport: `target/surefire-reports/org.bp.travel.SagaKafkaIntegrationTests.txt`.
