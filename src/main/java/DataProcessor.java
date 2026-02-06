import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;

/**
 * Консольная утилита для обработки структурированных данных из текстового файла. Программа считывает файл построчно,
 * проверяет корректность записей, разделяет их на валидные и невалидные, вычисляет агрегированную статистику и
 * формирует структурированный отчёт в консоль и выходной файл.
 * Ожидаемый формат записи - {@code идентификатор,название,числовое_значение}, где: идентификатор - положительное целое
 * число; название - непустая строка; числовое_значение - корректное десятичное число.
 */
public class DataProcessor {

    /**
     * Разделитель полей по умолчанию - запятая (формат CSV).
     */
    private static final String DEFAULT_DELIMITER = ",";

    /**
     * Ожидаемое количество полей в каждой записи.
     */
    private static final int EXPECTED_FIELDS = 3;

    /**
     * Представляет ошибку валидации одной строки входного файла.
     */
    static class ValidationError {
        private final int lineNumber;
        private final String lineContent;
        private final String reason;

        /**
         * Создаёт объект ошибки валидации.
         *
         * @param lineNumber  номер строки в исходном файле (начиная с 1)
         * @param lineContent содержимое строки (для отладки)
         * @param reason      описание причины ошибки на русском языке
         */
        public ValidationError(int lineNumber, String lineContent, String reason) {
            this.lineNumber = lineNumber;
            this.lineContent = (lineContent == null) ? "" : lineContent.trim();
            this.reason = Objects.requireNonNull(reason, "Причина ошибки не может быть null");
        }

        /**
         * Возвращает номер строки, в которой произошла ошибка.
         *
         * @return номер строки (≥1)
         */
        public int getLineNumber() {
            return lineNumber;
        }

        /**
         * Возвращает содержимое строки, вызвавшей ошибку.
         *
         * @return содержимое строки (без ведущих/замыкающих пробелов)
         */
        public String getLineContent() {
            return lineContent;
        }

        /**
         * Возвращает читаемое описание ошибки.
         *
         * @return описание ошибки на русском языке
         */
        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return String.format("Строка %d: \"%s\" - %s", lineNumber, lineContent, reason);
        }
    }

    /**
     * Содержит агрегированную статистику по обработанным данным.
     */
    static class Statistics {
        private int totalRecords = 0;
        private int validRecords = 0;
        private int invalidRecords = 0;
        private double sum = 0.0;
        private double min = Double.MAX_VALUE;
        private double max = Double.MIN_VALUE;
        private final List<ValidationError> errors = new ArrayList<>();

        /**
         * Добавляет корректную запись к статистике.
         *
         * @param value числовое значение из валидной записи
         */
        public void addValidRecord(double value) {
            validRecords++;
            sum += value;
            if (value < min) min = value;
            if (value > max) max = value;
        }

        /**
         * Добавляет ошибку валидации к списку проблемных записей.
         *
         * @param error объект ошибки
         */
        public void addError(ValidationError error) {
            invalidRecords++;
            errors.add(Objects.requireNonNull(error));
        }

        /**
         * Увеличивает счётчик общего количества записей.
         */
        public void incrementTotal() {
            totalRecords++;
        }

        public int getTotalRecords() {
            return totalRecords;
        }

        public int getValidRecords() {
            return validRecords;
        }

        public int getInvalidRecords() {
            return invalidRecords;
        }

        public double getSum() {
            return sum;
        }

        public double getMin() {
            return (validRecords > 0) ? min : 0.0;
        }

        public double getMax() {
            return (validRecords > 0) ? max : 0.0;
        }

        public double getAverage() {
            return (validRecords > 0) ? sum / validRecords : 0.0;
        }

        public List<ValidationError> getErrors() {
            return new ArrayList<>(errors);
        }
    }

    /**
     * Проверяет корректность одной строки данных.
     *
     * @param line       строка из входного файла
     * @param lineNumber номер строки (для сообщений об ошибках)
     * @param delimiter  символ-разделитель полей
     * @return {@link Optional#empty()}, если строка корректна; иначе - объект {@link ValidationError} с описанием
     * проблемы
     */
    private static Optional<ValidationError> validateLine(String line, int lineNumber, String delimiter) {
        if (line == null || line.trim().isEmpty()) {
            return Optional.of(new ValidationError(lineNumber, line, "Пустая строка"));
        }

        String[] fields = line.trim().split(delimiter, -1);

        if (fields.length != EXPECTED_FIELDS) {
            return Optional.of(new ValidationError(
                    lineNumber, line,
                    String.format("Неверное количество полей: ожидается %d, получено %d", EXPECTED_FIELDS, fields.length)
            ));
        }

        String idStr = fields[0].trim();
        String name = fields[1].trim();
        String valueStr = fields[2].trim();

        if (idStr.isEmpty()) return Optional.of(new ValidationError(lineNumber, line, "Пустой идентификатор"));
        if (name.isEmpty()) return Optional.of(new ValidationError(lineNumber, line, "Пустое название"));
        if (valueStr.isEmpty()) return Optional.of(new ValidationError(lineNumber, line, "Пустое числовое значение"));

        try {
            int id = Integer.parseInt(idStr);
            if (id <= 0) return Optional.of(new ValidationError(lineNumber, line,
                    "Идентификатор должен быть положительным целым числом"));
        } catch (NumberFormatException e) {
            return Optional.of(new ValidationError(lineNumber, line, "Идентификатор не является целым числом"));
        }

        try {
            double value = Double.parseDouble(valueStr);
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                return Optional.of(new ValidationError(lineNumber, line,
                        "Числовое значение является NaN или бесконечностью"));
            }
            if (!Double.isFinite(value)) {
                return Optional.of(new ValidationError(lineNumber, line,
                        "Числовое значение выходит за пределы допустимого диапазона"));
            }
        } catch (NumberFormatException e) {
            return Optional.of(new ValidationError(lineNumber, line,
                    "Числовое значение не является корректным числом"));
        }

        return Optional.empty();
    }

    /**
     * Обрабатывает входной файл и собирает статистику.
     *
     * @param inputPath путь к входному файлу (существующий)
     * @param delimiter символ-разделитель полей
     * @return объект {@link Statistics} с результатами обработки
     * @throws IOException              при ошибках чтения файла
     * @throws IllegalArgumentException если путь null
     */
    public static Statistics processFile(String inputPath, String delimiter) throws IOException {
        Objects.requireNonNull(inputPath, "Путь к входному файлу не может быть null");
        Objects.requireNonNull(delimiter, "Разделитель не может быть null");

        Statistics stats = new Statistics();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(inputPath), StandardCharsets.UTF_8))) {

            String line;
            int lineNumber = 0;

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                stats.incrementTotal();

                Optional<ValidationError> error = validateLine(line, lineNumber, delimiter);
                if (error.isPresent()) {
                    stats.addError(error.get());
                } else {
                    String valueStr = line.trim().split(delimiter, -1)[2].trim();
                    double value = Double.parseDouble(valueStr);
                    stats.addValidRecord(value);
                }
            }
        }
        return stats;
    }

    /**
     * Формирует читаемый отчёт на основе собранной статистики. Числа форматируются с точкой как десятичным разделителем
     * независимо от локали системы.
     *
     * @param stats объект статистики
     * @return строка с полным отчётом
     */
    public static String generateReport(Statistics stats) {
        DecimalFormatSymbols symbols = new DecimalFormatSymbols(Locale.US);
        DecimalFormat df = new DecimalFormat("#0.00", symbols);

        StringBuilder sb = new StringBuilder();
        sb.append("=".repeat(60)).append("\n");
        sb.append("ОТЧЁТ ОБРАБОТКИ ДАННЫХ\n");
        sb.append("=".repeat(60)).append("\n\n");

        sb.append(String.format("Общее количество записей: %d%n", stats.getTotalRecords()));
        sb.append(String.format("Корректных записей: %d%n", stats.getValidRecords()));
        sb.append(String.format("Некорректных записей: %d%n", stats.getInvalidRecords()));
        sb.append("\n");

        if (stats.getValidRecords() > 0) {
            sb.append(String.format("Сумма: %s%n", df.format(stats.getSum())));
            sb.append(String.format("Минимум: %s%n", df.format(stats.getMin())));
            sb.append(String.format("Максимум: %s%n", df.format(stats.getMax())));
            sb.append(String.format("Среднее: %s%n", df.format(stats.getAverage())));
            sb.append("\n");
        }

        if (!stats.getErrors().isEmpty()) {
            sb.append("Обнаруженные ошибки:\n");
            List<ValidationError> errors = stats.getErrors();
            for (int i = 0; i < errors.size(); i++) {
                sb.append(String.format("  %d. %s%n", i + 1, errors.get(i)));
            }
        } else {
            sb.append("Ошибок не обнаружено. Все данные корректны.\n");
        }

        sb.append("\n").append("=".repeat(60)).append("\n");
        sb.append(String.format("Отчёт сформирован: %s%n", java.time.LocalDateTime.now()));
        sb.append("=".repeat(60));

        return sb.toString();
    }

    /**
     * Точка входа в консольное приложение. Запрашивает у пользователя - путь к входному файлу, путь к выходному файлу
     * (по умолчанию - report.txt), разделитель полей (по умолчанию - запятая).
     *
     * @param args аргументы командной строки
     */
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("=== Утилита обработки данных ===");
        System.out.print("Введите путь к входному файлу: ");
        String inputPath = scanner.nextLine().trim();

        if (inputPath.isEmpty()) {
            System.err.println("Ошибка: путь к файлу не может быть пустым.");
            return;
        }

        System.out.print("Введите путь к выходному файлу (Enter для 'report.txt'): ");
        String outputPath = scanner.nextLine().trim();
        if (outputPath.isEmpty()) {
            outputPath = "report.txt";
        }

        System.out.print("Введите разделитель (Enter для запятой ','): ");
        String delimiter = scanner.nextLine().trim();
        if (delimiter.isEmpty()) {
            delimiter = DEFAULT_DELIMITER;
        } else if ("\\t".equals(delimiter)) {
            delimiter = "\t";
        }

        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.err.println("Ошибка: файл не найден - " + inputPath);
            return;
        }
        if (!inputFile.isFile()) {
            System.err.println("Ошибка: указанный путь не является файлом - " + inputPath);
            return;
        }
        if (!inputFile.canRead()) {
            System.err.println("Ошибка: нет прав на чтение файла - " + inputPath);
            return;
        }

        try {
            Statistics stats = processFile(inputPath, delimiter);
            String report = generateReport(stats);

            System.out.println("\n" + report);

            try (PrintWriter writer = new PrintWriter(
                    new OutputStreamWriter(new FileOutputStream(outputPath), StandardCharsets.UTF_8))) {
                writer.print(report);
            }

            System.out.println("\nОтчёт успешно сохранён в файл: " + new File(outputPath).getAbsolutePath());

        } catch (IOException e) {
            System.err.println("Ошибка ввода-вывода: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Неожиданная ошибка: " + e.getMessage());
            e.printStackTrace();
        } finally {
            scanner.close();
        }
    }
}