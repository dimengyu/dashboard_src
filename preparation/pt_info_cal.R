library(tidyverse)
library(haven)

dt <- read_sas("outcome_pt.sas7bdat") %>% 
  mutate(AGE = case_when(INC_AGE < 30 ~ "18-29",
                         INC_AGE < 50 ~ "30-49", 
                         INC_AGE < 65 ~ "50-64", 
                         T ~ "65-80"),
         SEX = case_when(SEX == "1" ~ "Male", 
                         SEX == "2" ~ "Female"),
         RACE = factor(as.numeric(RACE), levels = c(1:6, 9),
                       labels = c("White", "Black", "AI/AN", "Asian", "NHPI", "Other", "Unknown")),
         HISPANIC = factor(HISPANIC, levels = c("2", "1", "9"), labels = c("Non-Hispanic", "Hispanic", "Unknown")),
         insurance_esrd = ifelse(is.na(insurance_esrd), 6, insurance_esrd),
         INSURANCE = factor(insurance_esrd, levels = 1:6, labels = c("Medicaid", "Medicare", "Employer", "Other", "No Insurance", "Unknown"))) %>%
  select(nw, tx_ctr, preempt_rf, rf, rf_1yr, eval, eval_3m, eval_6m, wl, wl_6m, wl_2yr,
         RACE, SEX, HISPANIC, INC_AGE, AGE, INSURANCE)

dt_nopre <- dt %>% filter(preempt_rf != 1)

saveRDS(dt, "pt_info.rds")
saveRDS(dt_nopre, "pt_info_nopre.rds")