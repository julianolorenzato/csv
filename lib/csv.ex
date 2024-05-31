defmodule Csv do
  @spec parse(binary()) :: {:ok, [map()]} | {:error, String.t()}
  def parse(filepath) do
    with {:ok, file} <- open_file(filepath),
         {:ok, headers} <- read_headers(file) do
      read_rows(file, headers)
    end
  end

  @spec open_file(binary()) :: {:ok, IO.device()} | {:error, String.t()}
  defp open_file(filepath) do
    case File.open(filepath, [:read]) do
      {:ok, file} -> {:ok, file}
      {:error, :enoent} -> {:error, "File not found"}
      {:error, :eisdir} -> {:error, "File not found"}
    end
  end

  @spec read_headers(IO.device()) :: {:ok, [String.t()]} | {:error, String.t()}
  defp read_headers(file) do
    case IO.read(file, :line) do
      :eof -> {:error, "File is empty"}
      first_line -> {:ok, split_line(first_line)}
    end
  end

  @spec read_rows(IO.device(), [String.t()]) :: {:ok, [map()]} | {:error, String.t()}
  defp read_rows(file, headers) do
    result =
      IO.stream(file, :line)
      |> Task.async_stream(&process_line(&1, headers))
      |> Stream.map(&unwrap_result/1)
      |> Enum.reduce_while([], fn
        {:ok, map}, acc -> {:cont, [map | acc]}
        {:error, _} = error, _ -> {:halt, error}
      end)

    if is_list(result) do
      {:ok, Enum.reverse(result)}
    else
      result
    end
  end

  @spec process_line(String.t(), [String.t()]) :: {:ok, map()} | {:error, String.t()}
  defp process_line(line, headers) do
    values = split_line(line)

    if length(headers) == length(values) do
      {:ok,
       headers
       |> Enum.zip(values)
       |> Enum.into(%{})}
    else
      {:error, "Invalid CSV"}
    end
  end

  @spec unwrap_result({:ok, any()}) :: any()
  def unwrap_result({:ok, result}), do: result

  @spec split_line(String.t()) :: [String.t()]
  defp split_line(line) do
    line
    |> String.split(",")
    |> Enum.map(&String.trim/1)
  end
end
