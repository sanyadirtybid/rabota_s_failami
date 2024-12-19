const fs = require('fs');
const zlib = require('zlib');
const path = require('path');
const csvWriter = require('csv-writer').createObjectCsvWriter;

/**
 * Извлекает .gz файл в JSON файл
 * @param {string} gzFilePath - Путь к .gz файлу
 * @param {string} outputDir - Директория для сохранения извлечённого файла
 * @returns {string} - Путь к извлечённому JSON файлу
 */
function extractGzFile(gzFilePath, outputDir) {
    const fileName = path.basename(gzFilePath, '.gz');
    const outputPath = path.join(outputDir, fileName);

    const fileContents = fs.createReadStream(gzFilePath);
    const writeStream = fs.createWriteStream(outputPath);
    const unzip = zlib.createGunzip();

    fileContents.pipe(unzip).pipe(writeStream);

    return new Promise((resolve, reject) => {
        writeStream.on('finish', () => resolve(outputPath));
        writeStream.on('error', reject);
    });
}

// Функция для записи данных в CSV
function writeCsv(filePath, data, headers) {
    const writer = csvWriter({
        path: filePath,
        header: headers
    });

    writer.writeRecords(data)
        .then(() => console.log(`Данные записаны в ${filePath}`))
        .catch((err) => console.error('Ошибка записи в CSV:', err));
}

// Главная программа
(async function main() {
    const args = process.argv.slice(2);

    if (args.length < 2) {
        console.log('Использование: node index.js <gzFilePath> <outputDir>');
        process.exit(1);
    }

    const [gzFilePath, outputDir] = args;

    try {
        if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir, { recursive: true });
        }

        console.log(`Извлечение ${gzFilePath}...`);
        const extractedFilePath = await extractGzFile(gzFilePath, outputDir);
        console.log(`Файл извлечен в ${extractedFilePath}`);
        
        const jsonData = fs.readFileSync(extractedFilePath, 'utf-8');
        const fixedJsonData = `[${jsonData.split('\n').join(',')}]`;
        const fixedJson = fixedJsonData.replace(/,(\s*[\]}])/g, '$1');
        
        const parsedData = JSON.parse(fixedJson);
        console.log('Парсинг JSON данных:', parsedData.slice(0, 5)); // Печатаем первые 5 записей

        // 1. Список продуктов от самого популярного
        const productPopularity = {};
        parsedData.forEach((review) => {
            const asin = review.asin;
            if (productPopularity[asin]) {
                productPopularity[asin]++;
            } else {
                productPopularity[asin] = 1;
            }
        });
        
        const sortedByPopularity = Object.entries(productPopularity)
            .map(([asin, count]) => ({ asin, count }))
            .sort((a, b) => b.count - a.count);
        
        // Запись в CSV
        writeCsv(path.join(outputDir, 'products_by_popularity.csv'), sortedByPopularity, [
            { id: 'asin', title: 'ASIN' },
            { id: 'count', title: 'Review Count' }
        ]);

        // 2. Список продуктов по рейтингу (с весом отзыва)
        const productRatings = {};
        parsedData.forEach((review) => {
            const asin = review.asin;
            if (!productRatings[asin]) {
                productRatings[asin] = { totalRating: 0, totalReviews: 0 };
            }
            productRatings[asin].totalRating += review.overall;
            productRatings[asin].totalReviews++;
        });

        const sortedByRating = Object.entries(productRatings)
            .map(([asin, { totalRating, totalReviews }]) => ({
                asin,
                averageRating: totalRating / totalReviews
            }))
            .sort((a, b) => b.averageRating - a.averageRating);

        // Запись в CSV
        writeCsv(path.join(outputDir, 'products_by_rating.csv'), sortedByRating, [
            { id: 'asin', title: 'ASIN' },
            { id: 'averageRating', title: 'Average Rating' }
        ]);

        // 3. Товары за определённый период
        const popularInPeriod = parsedData.filter((review) => {
            const reviewDate = new Date(review.reviewTime);
            return reviewDate.getFullYear() === 2015 && reviewDate.getMonth() === 3; // Пример: апрель 2015
        });

        const popularInPeriodByAsin = {};
        popularInPeriod.forEach((review) => {
            const asin = review.asin;
            if (popularInPeriodByAsin[asin]) {
                popularInPeriodByAsin[asin]++;
            } else {
                popularInPeriodByAsin[asin] = 1;
            }
        });

        const sortedByPeriod = Object.entries(popularInPeriodByAsin)
            .map(([asin, count]) => ({ asin, count }))
            .sort((a, b) => b.count - a.count);

        // Запись в CSV
        writeCsv(path.join(outputDir, 'popular_in_period.csv'), sortedByPeriod, [
            { id: 'asin', title: 'ASIN' },
            { id: 'count', title: 'Review Count' }
        ]);

        // 4. Поиск товара по тексту отзыва
        const searchText = 'great'; // Пример поиска
        const searchResults = parsedData.filter((review) => {
            // Проверяем, существует ли reviewText и является ли он строкой
            if (review.reviewText && typeof review.reviewText === 'string') {
                return review.reviewText.toLowerCase().includes(searchText.toLowerCase());
            }
            return false;
        });

        // Запись результатов поиска в CSV
        writeCsv(path.join(outputDir, 'search_results.csv'), searchResults, [
            { id: 'asin', title: 'ASIN' },
            { id: 'reviewerName', title: 'Reviewer Name' },
            { id: 'reviewText', title: 'Review Text' }
        ]);

    } catch (err) {
        console.error('Ошибка:', err.message);
    }
})();
