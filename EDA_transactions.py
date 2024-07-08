# Импортируем необходимые библиотеки
import matplotlib.pyplot as plt
import pandas as pd

# Загружаем данные
df = pd.read_csv("transactions.csv")

# Проверка наличия пропущенных значений
if df.isnull().sum().sum() != 0:
    # Удаляем записи с пропущенными значениями
    df = df.dropna()

# Преобразуем столбец date в формат datetime
df["date"] = pd.to_datetime(df["date"])


# Вычисляем общее количество транзакций
total_transactions = df["transaction_id"].count()
print("Общее количество транзакций:", total_transactions)

# Определяем общее количество уникальных клиентов
unique_customers = df["customer_id"].nunique()
print("Общее количество уникальных клиентов:", unique_customers)

# Находим топ-5 категорий продуктов по общему доходу
top_five_categories = df.groupby("product_category")["amount"].sum().nlargest(5)
print("Топ-5 категорий продуктов по общему доходу:")
print(top_five_categories)

# Вычисляем среднюю сумму транзакции по каждой категории продуктов
average_transaction_amount = df.groupby("product_category")["amount"].mean()
print("Средняя сумма транзакции по каждой категории продуктов:")
print(average_transaction_amount)


# Гистограмма распределения количества транзакций по месяцам
monthly_transactions = df["date"].dt.month.value_counts().sort_index()

plt.figure(figsize=(10, 6))
monthly_transactions.plot(kind="bar", color="firebrick")
plt.xlabel("Месяц")
plt.ylabel("Количество транзакций")
plt.title("Распределение количества транзакций по месяцам")
plt.xticks(rotation=0)
plt.grid(axis="y", linestyle="--", alpha=0.6)
plt.show()

# Круговая диаграмма доли дохода по категориям продуктов
revenue_by_category = df.groupby("product_category")["amount"].sum()

plt.figure(figsize=(8, 8))
plt.pie(
    revenue_by_category,
    labels=revenue_by_category.index,
    autopct="%1.1f%%",
    wedgeprops={"linewidth": 1.5, "edgecolor": "white"},
    textprops={"fontsize": 14},
)
plt.axis("equal")
plt.title("Доля дохода по категориям продуктов", fontdict={"fontsize": 20}, pad=30)
plt.show()


# Создаем новый столбец year_month, содержащий год и месяц транзакции
df["year_month"] = df["date"].dt.strftime("%Y-%m")

# Сводная таблица с общим доходом по каждому клиенту для каждого месяца
pivot_table = df.pivot_table(
    index="customer_id",
    columns="year_month",
    values="amount",
    aggfunc="sum",
    fill_value=0,
)

# Сохраняем сводную таблицу в новый CSV-файл
pivot_table.to_csv("tmp/task2/monthly_revenue_per_customer.csv", index=False)
