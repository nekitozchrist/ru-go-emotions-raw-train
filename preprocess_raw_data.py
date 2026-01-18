import os
import yaml
import pandas as pd
from pathlib import Path
from config import config  # Это модуль с путями, можно убрать, если не нужен

def preload_raw_data() -> pd.DataFrame:
	""" Вводные пути и обработка данных с сохранением, удалением и проверкой перезаписи """
	# Папка
	data_dir = Path(config['dataset_dir']) / "dataset"
	# Файлы
	raw_file = Path(data_dir) / "ru-go-emotions-raw.csv"
	id_to_label_file = Path(data_dir) / "id_to_label.yaml"
	final_file = Path(data_dir) / "ru-go-emotions-raw-train.csv"
	temp_labels_file = Path(data_dir) / "labels.csv"  # временный файл
	final_columns = ['ru_text', 'text', 'labels', 'id'] # разметка
	# Проверка существования итогового файла

	print(f"\nБудет создан новый файл .csv с разметкой:\n {final_columns}")

	if final_file.exists():
		answer = input(f"\nФайл уже существует. Перезаписать? (y/n): ").strip().lower()
		if answer != 'y':
			print("\nОперация отменена пользователем.\n")
			print(f"Файл уже существует: {final_file}\n")
			return


	print("\nЗапущен процесс обработки меток и создания нового файла...\n")

	try:
		# Загружаем DataFrame
		raw_df = pd.read_csv(raw_file)
	except Exception as e:
		print(f"\nОшибка при чтении файла {raw_file}: {e}\n")
		return

	# Загружаем YAML
	try:
		with open(id_to_label_file, 'r', encoding='utf-8') as f:
			id_to_label = yaml.safe_load(f)
		id_to_label_int = {int(k): v for k, v in id_to_label.items()}
	except Exception as e:
		print(f"\nОшибка при чтении YAML файла {id_to_label_file}: {e}\n")
		return

	# Создаем список пар (ID, название), отсортированный по ID
	id_label_pairs = sorted(id_to_label_int.items(), key=lambda x: x[0])
	emotion_ids = [k for k, v in id_label_pairs]
	label_to_id = {v: k for k, v in id_to_label_int.items()}

	# Создаем колонку 'labels'
	labels_list = []

	for _, row in raw_df.iterrows():
		selected_ids = []
		for col_name in raw_df.columns:
			if col_name == 'ru_text':
				continue
			if col_name in label_to_id:
				if row[col_name] == 1:
					selected_ids.append(label_to_id[col_name])
		labels_str = '[' + ' '.join(map(str, sorted(selected_ids))) + ']'
		labels_list.append(labels_str)

	raw_df['labels'] = labels_list

	# Сохраняем во временный файл
	try:
		raw_df.to_csv(temp_labels_file, index=False)
	except Exception as e:
		print(f"\nОшибка при сохранении временного файла {temp_labels_file}: {e}\n")
		return

	# Читаем временный файл и сохраняем только нужные колонки
	try:
		df = pd.read_csv(temp_labels_file)
		final_df = df[final_columns]
		final_df.to_csv(final_file, index=False)
		print(f"\nСоздан файл {final_file}")
	except Exception as e:
		print(f"\nОшибка при обработке конечного файла: {e}\n")
		return

	# Удаление временного файла с обработанной меткой
	try:
		os.remove(temp_labels_file)
	except FileNotFoundError:
		print("\nВременный файл не найден, возможно уже удален.\n")
	except Exception as e:
		print(f"\nОшибка при удалении временного файла: {e}\n")


	return

if __name__ == "__main__":
	preload_raw_data()
