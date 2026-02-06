import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

public class DataProcessorTests {

    @TempDir
    Path tempDir;

    private Path createTempFile(String filename, String content) throws IOException {
        Path file = tempDir.resolve(filename);
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file.toFile()), StandardCharsets.UTF_8))) {
            writer.write(content);
        }
        return file;
    }

    @Test
    @DisplayName("Корректная запись должна проходить валидацию")
    void validRecordTest() throws IOException {
        String content = "1,Товар A,100.50\n2,Товар B,200.75";
        Path inputFile = createTempFile("valid.csv", content);

        DataProcessor.Statistics stats = DataProcessor.processFile(inputFile.toString(), ",");

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(stats.getTotalRecords()).as("Общее количество записей").isEqualTo(2);
        softly.assertThat(stats.getValidRecords()).as("Корректные записи").isEqualTo(2);
        softly.assertThat(stats.getInvalidRecords()).as("Некорректные записи").isEqualTo(0);
        softly.assertThat(stats.getSum()).as("Сумма").isCloseTo(301.25, within(0.01));
        softly.assertThat(stats.getMin()).as("Минимум").isCloseTo(100.50, within(0.01));
        softly.assertThat(stats.getMax()).as("Максимум").isCloseTo(200.75, within(0.01));
        softly.assertThat(stats.getAverage()).as("Среднее").isCloseTo(150.625, within(0.01));
    }

    @Test
    @DisplayName("Пустая строка должна считаться ошибкой")
    void emptyLineTest() throws IOException {
        String content = "1,Товар A,100.50\n\n2,Товар B,200.75";
        Path inputFile = createTempFile("empty_line.csv", content);

        DataProcessor.Statistics stats = DataProcessor.processFile(inputFile.toString(), ",");
        List<DataProcessor.ValidationError> errors = stats.getErrors();

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(stats.getTotalRecords()).isEqualTo(3);
        softly.assertThat(stats.getValidRecords()).isEqualTo(2);
        softly.assertThat(stats.getInvalidRecords()).isEqualTo(1);
        softly.assertThat(errors).hasSize(1);
        softly.assertThat(errors.get(0).getReason()).contains("Пустая строка");
    }

    @Test
    @DisplayName("Некорректное количество полей должно вызывать ошибку")
    void wrongFieldCountTest() throws IOException {
        String content = "1,Товар A,100.50,extra_field\n2,Товар B";
        Path inputFile = createTempFile("wrong_fields.csv", content);

        DataProcessor.Statistics stats = DataProcessor.processFile(inputFile.toString(), ",");
        List<DataProcessor.ValidationError> errors = stats.getErrors();

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(stats.getTotalRecords()).isEqualTo(2);
        softly.assertThat(stats.getValidRecords()).isEqualTo(0);
        softly.assertThat(stats.getInvalidRecords()).isEqualTo(2);
        softly.assertThat(errors).hasSize(2);
        softly.assertThat(errors.get(0).getReason()).contains("Неверное количество полей");
        softly.assertThat(errors.get(1).getReason()).contains("Неверное количество полей");
}

    @Test
    @DisplayName("Пустые поля должны вызывать соответствующие ошибки")
    void emptyFieldsTest() throws IOException {
        String content = ",Товар A,100.50\n2,,200.75\n3,Товар C,";
        Path inputFile = createTempFile("empty_fields.csv", content);

        DataProcessor.Statistics stats = DataProcessor.processFile(inputFile.toString(), ",");
        List<DataProcessor.ValidationError> errors = stats.getErrors();

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(stats.getTotalRecords()).isEqualTo(3);
        softly.assertThat(stats.getValidRecords()).isEqualTo(0);
        softly.assertThat(stats.getInvalidRecords()).isEqualTo(3);
        softly.assertThat(errors).hasSize(3);
        softly.assertThat(errors.get(0).getReason()).contains("Пустой идентификатор");
        softly.assertThat(errors.get(1).getReason()).contains("Пустое название");
        softly.assertThat(errors.get(2).getReason()).contains("Пустое числовое значение");
    }

    @Test
    @DisplayName("Нечисловые значения должны вызывать ошибки")
    void nonNumericValuesTest() throws IOException {
        String content = "abc,Товар A,100.50\n2,Товар B,некорректно";
        Path inputFile = createTempFile("non_numeric.csv", content);

        DataProcessor.Statistics stats = DataProcessor.processFile(inputFile.toString(), ",");
        List<DataProcessor.ValidationError> errors = stats.getErrors();

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(stats.getTotalRecords()).isEqualTo(2);
        softly.assertThat(stats.getValidRecords()).isEqualTo(0);
        softly.assertThat(stats.getInvalidRecords()).isEqualTo(2);
        softly.assertThat(errors).hasSize(2);
        softly.assertThat(errors.get(0).getReason()).contains("Идентификатор не является целым числом");
        softly.assertThat(errors.get(1).getReason()).contains("Числовое значение не является корректным числом");
    }

    @Test
    @DisplayName("Файл без заголовка должен обрабатываться корректно")
    void noHeaderTest() throws IOException {
        String content = "1,Товар A,100.50\n2,Товар B,200.75";
        Path inputFile = createTempFile("no_header.csv", content);

        DataProcessor.Statistics stats = DataProcessor.processFile(inputFile.toString(), ",");

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(stats.getTotalRecords()).isEqualTo(2);
        softly.assertThat(stats.getValidRecords()).isEqualTo(2);
    }

    @Test
    @DisplayName("Поддержка другого разделителя (помимо default)")
    void semicolonDelimiterTest() throws IOException {
        String content = "1;Товар A;100.50\n2;Товар B;200.75";
        Path inputFile = createTempFile("semicolon.csv", content);

        DataProcessor.Statistics stats = DataProcessor.processFile(inputFile.toString(), ";");

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(stats.getTotalRecords()).isEqualTo(2);
        softly.assertThat(stats.getValidRecords()).isEqualTo(2);
        softly.assertThat(stats.getSum()).isCloseTo(301.25, within(0.01));
    }

    @Test
    @DisplayName("Отрицательный ID должен быть недопустим")
    void negativeIdTest() throws IOException {
        String content = "-1,Товар A,100.50";
        Path inputFile = createTempFile("negative_id.csv", content);

        DataProcessor.Statistics stats = DataProcessor.processFile(inputFile.toString(), ",");

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(stats.getTotalRecords()).isEqualTo(1);
        softly.assertThat(stats.getValidRecords()).isEqualTo(0);
        softly.assertThat(stats.getInvalidRecords()).isEqualTo(1);
        softly.assertThat(stats.getErrors().get(0).getReason()).contains("положительным");
    }

    @Test
    @DisplayName("Генерация отчёта должна содержать все необходимые элементы")
    void reportGenerationTest() throws IOException {
        String content = "1,Товар A,100.50\n2,,200.75";
        Path inputFile = createTempFile("report_test.csv", content);

        DataProcessor.Statistics stats = DataProcessor.processFile(inputFile.toString(), ",");
        String report = DataProcessor.generateReport(stats);

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(report).contains("Общее количество записей: 2");
        softly.assertThat(report).contains("Корректных записей: 1");
        softly.assertThat(report).contains("Некорректных записей: 1");
        softly.assertThat(report).contains("Сумма: 100.50");
        softly.assertThat(report).contains("Строка 2: \"2,,200.75\" - Пустое название");
        softly.assertThat(report).contains("Отчёт сформирован:");
    }

    @Test
    @DisplayName("Пустой файл должен обрабатываться без ошибок")
    void emptyFileTest() throws IOException {
        Path inputFile = createTempFile("empty.csv", "");

        DataProcessor.Statistics stats = DataProcessor.processFile(inputFile.toString(), ",");

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(stats.getTotalRecords()).isEqualTo(0);
        softly.assertThat(stats.getValidRecords()).isEqualTo(0);
        softly.assertThat(stats.getInvalidRecords()).isEqualTo(0);
    }

    private static org.assertj.core.data.Offset<Double> within(double delta) {
        return org.assertj.core.data.Offset.offset(delta);
    }
}