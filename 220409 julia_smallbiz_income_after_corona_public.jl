using DataFrames, CSV, Distributions, LinearAlgebra, Statistics, VegaLite

# 1. 데이터 읽어들이기(csv파일 경로 재설정 필요합니다)
A = CSV.read("220409_smallbiz_after_corona.csv", 
    DataFrame, header=true) # 가계동향조사 2019~2021년 자료를 토대로 구축한 longform 데이터프레임

# 균등화 가구경상소득 변수 설정
function populate_staradardized_variables(data)
    std_income(total, family) = total ./ sqrt.(family)
    data_std = transform(data, [:total, :family] => std_income => :std_total_income)
    return data_std
end

Astd = dropmissing(populate_staradardized_variables(A), 
    :job) # 균등화가구경상소득 추가, 종사자 무응답 제외 자료 구축

# 가중치의 1/10을 반영해 데이터프레임을 생성하는 함수 구축
## 가계동향조사는 응답 가구별 가중치가 존재함
### 처리 속도 향상을 위해 가중치를 10으로 나누어 반영
function populate_weighted_dataframe(data)
    df = data
    insertcols!(df, 1, :id => string.(df.num))
    df_weighted = DataFrame()
    for row in 1:nrow(df)
        for i in 1:(Int(round(df.weight[row]/10)))
            addrow = df[row,:]
            addrow.id = string(addrow.num) * ".$i"
            push!(df_weighted, addrow)
        end
    end
    return df_weighted 
end

# 각 연도별 하반기 자영업자 소득을 추출
Astd_34q_smallbiz = filter([:quarter, :job] => (x, y) -> (
    x == 201944 || x == 202044 || x == 202144 || x == 201934 || x == 202034 ||
    x == 202134) && (y .== 4 || y .== 5), Astd)
divide100(num) = Int.(round.(num ./ 100)) # 분기 변수값을 연도 변수값으로 변환하는 함수
Astd_34q_smallbiz = transform(Astd_34q_smallbiz, :quarter => divide100 => :year)

# 3, 4분기 비교
Astd_34q_smallbiz_weighted = populate_weighted_dataframe(Astd_34q_smallbiz)

## 연도별 전체 가구경상소득 평균
trend_smallbiz_all = combine(groupby(Astd_34q_smallbiz_weighted, :year), :total => mean)

trend_smallbiz_all |> @vlplot( # 가구경상소득 그래프
    :bar,
    height = 300,
    width = 200,
    x = {"year:o", title = "연도"},
    y = {:total_mean, title = "월평균 가구경상소득"},
    color = {:year, legend = nothing},
    title = "자영업 가구소득 추이(2019~2021년 하반기)"
)

Astd_34q_smallbiz_incomeclass = groupby(Astd_34q_smallbiz_weighted,
    :incomeclass)

df_trend = DataFrame()
for i in 1:5
    incomeclass = Astd_34q_smallbiz_incomeclass[i]
    class = groupby(incomeclass, :year)
    trend_class = combine(class, [:std_total_income, 
    :all, :total, :socialincome, :biz, :labor] .=> mean)
    insertcols!(trend_class, 1, :class => "$i 분위")
    df_trend = vcat(df_trend, trend_class)
end
df_trend

## 총소득
df_trend |> @vlplot(
    :height = 200,
    :width = 1000,
    :bar,
    x = {"year:o", title = ""},
    y = {"all_mean:q", title = "가구 총소득"},
    column = {"class:o", title = "하반기 자영업자 계층별 평균 가구 총소득"},
    color = {"year:o", legend=nothing}
) 

## 균등화경상소득
df_trend |> @vlplot(
    :height = 200,
    :width = 1000,
    :bar,
    x = {"year:o", title = ""},
    y = {"std_total_income_mean", title = "가구균등화 경상소득"},
    column = {"class:o", title = "하반기 자영업자 계층별 평균 가구경상소득"},
    color = {"year:o", legend=nothing}
)

## 경상소득
df_trend |> @vlplot(
    :height = 200,
    :width = 1000,
    :bar,
    x = {"year:o", title = ""},
    y = {"total_mean", title = "가구 경상소득"},
    column = {"class:o", title = "하반기 자영업자 계층별 평균 가구경상소득"},
    color = {"year:o", legend=nothing}
)


## 사회수혜금
df_trend |> @vlplot(
    :height = 200,
    :width = 1000,
    :bar,
    x = {"year:o", title = ""},
    y = {"socialincome_mean", title = "사회수혜금"},
    column = {"class:o", title = "하반기 자영업자 계층별 사회수혜금"},
    color = {"year:o", legend=nothing}
)

## 사업소득
df_trend |> @vlplot(
    :height = 200,
    :width = 1000,
    :bar,
    x = {"year:o", title = ""},
    y = {"biz_mean", title = "사업소득"},
    column = {"class:o", title = "하반기 자영업자 계층별 사업소득"},
    color = {"year:o", legend=nothing}
)

## 근로소득
df_trend |> @vlplot(
    :height = 200,
    :width = 1000,
    :bar,
    x = {"year:o", title = ""},
    y = {"labor_mean", title = "근로소득"},
    column = {"class:o", title = "하반기 자영업자 계층별 근로소득"},
    color = {"year:o", legend=nothing}
)
