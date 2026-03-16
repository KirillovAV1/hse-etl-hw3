**Кириллов Александр | Вебинар 3 ДЗ (практическое)**

**Дисциплина**: «ETL- процессы»

**Тема**: «Основы трансформации данных»

**Цель задания**: Отработать навыки преобразования данных.

**Результат работы пайплайна**:

![_EyCh1WUiD0NszNTJ_5zuXkbwD9S6kFsnWp_ER83yD0QfxA_z0EPvEijxC3Mp1OCNETlceW-f0DbGCyeqvcBM0Zt](https://github.com/user-attachments/assets/287e200a-02f9-41cc-8a1d-c7f143939eae)

Все преобразованные данные загружались в заранее созданную таблицу:

```sql
create type out_or_in as enum('Out','In');

create table temperature (
	id VARCHAR(255) primary key,
	room_id  VARCHAR(30),
	noted_date DATE,
	temperature INT,
	out_in out_or_in
);
```
