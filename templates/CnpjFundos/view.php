<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\CnpjFundo $cnpjFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Cnpj Fundo'), ['action' => 'edit', $cnpjFundo->id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Cnpj Fundo'), ['action' => 'delete', $cnpjFundo->id], ['confirm' => __('Are you sure you want to delete # {0}?', $cnpjFundo->id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Cnpj Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Cnpj Fundo'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="cnpjFundos view content">
            <h3><?= h($cnpjFundo->id) ?></h3>
            <table>
                <tr>
                    <th><?= __('CNPJ') ?></th>
                    <td><?= h($cnpjFundo->CNPJ) ?></td>
                </tr>
                <tr>
                    <th><?= __('DENOM SOCIAL') ?></th>
                    <td><?= h($cnpjFundo->DENOM_SOCIAL) ?></td>
                </tr>
                <tr>
                    <th><?= __('Id') ?></th>
                    <td><?= $this->Number->format($cnpjFundo->id) ?></td>
                </tr>
                <tr>
                    <th><?= __('DT REG CVM') ?></th>
                    <td><?= h($cnpjFundo->DT_REG_CVM) ?></td>
                </tr>
            </table>
            <div class="related">
                <h4><?= __('Related Cadastro Fundos') ?></h4>
                <?php if (!empty($cnpjFundo->cadastro_fundos)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Cnpj Fundo Id') ?></th>
                            <th><?= __('DT CONST') ?></th>
                            <th><?= __('Tipo Classe Fundo Id') ?></th>
                            <th><?= __('DT INI CLASSE') ?></th>
                            <th><?= __('Tipo Rentabilidade Fundo Id') ?></th>
                            <th><?= __('CONDOM ABERTO') ?></th>
                            <th><?= __('FUNDO COTAS') ?></th>
                            <th><?= __('FUNDO EXCLUSIVO') ?></th>
                            <th><?= __('INVEST QUALIF') ?></th>
                            <th><?= __('Gestor Fundo Id') ?></th>
                            <th><?= __('Administrador Fundo Id') ?></th>
                            <th><?= __('DT REG CVM') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($cnpjFundo->cadastro_fundos as $cadastroFundos) : ?>
                        <tr>
                            <td><?= h($cadastroFundos->cnpj_fundo_id) ?></td>
                            <td><?= h($cadastroFundos->DT_CONST) ?></td>
                            <td><?= h($cadastroFundos->tipo_classe_fundo_id) ?></td>
                            <td><?= h($cadastroFundos->DT_INI_CLASSE) ?></td>
                            <td><?= h($cadastroFundos->tipo_rentabilidade_fundo_id) ?></td>
                            <td><?= h($cadastroFundos->CONDOM_ABERTO) ?></td>
                            <td><?= h($cadastroFundos->FUNDO_COTAS) ?></td>
                            <td><?= h($cadastroFundos->FUNDO_EXCLUSIVO) ?></td>
                            <td><?= h($cadastroFundos->INVEST_QUALIF) ?></td>
                            <td><?= h($cadastroFundos->gestor_fundo_id) ?></td>
                            <td><?= h($cadastroFundos->administrador_fundo_id) ?></td>
                            <td><?= h($cadastroFundos->DT_REG_CVM) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'CadastroFundos', 'action' => 'view', $cadastroFundos->cnpj_fundo_id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'CadastroFundos', 'action' => 'edit', $cadastroFundos->cnpj_fundo_id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'CadastroFundos', 'action' => 'delete', $cadastroFundos->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $cadastroFundos->cnpj_fundo_id)]) ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    </table>
                </div>
                <?php endif; ?>
            </div>
            <div class="related">
                <h4><?= __('Related Cancelamento Fundos') ?></h4>
                <?php if (!empty($cnpjFundo->cancelamento_fundos)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Cnpj Fundo Id') ?></th>
                            <th><?= __('DT CANCEL') ?></th>
                            <th><?= __('DT REG CVM') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($cnpjFundo->cancelamento_fundos as $cancelamentoFundos) : ?>
                        <tr>
                            <td><?= h($cancelamentoFundos->cnpj_fundo_id) ?></td>
                            <td><?= h($cancelamentoFundos->DT_CANCEL) ?></td>
                            <td><?= h($cancelamentoFundos->DT_REG_CVM) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'CancelamentoFundos', 'action' => 'view', $cancelamentoFundos->cnpj_fundo_id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'CancelamentoFundos', 'action' => 'edit', $cancelamentoFundos->cnpj_fundo_id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'CancelamentoFundos', 'action' => 'delete', $cancelamentoFundos->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $cancelamentoFundos->cnpj_fundo_id)]) ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    </table>
                </div>
                <?php endif; ?>
            </div>
            <div class="related">
                <h4><?= __('Related Doc Extratos Fundos') ?></h4>
                <?php if (!empty($cnpjFundo->doc_extratos_fundos)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Cnpj Fundo Id') ?></th>
                            <th><?= __('DT COMPTC') ?></th>
                            <th><?= __('Tipo Anbima Classe Id') ?></th>
                            <th><?= __('APLIC MIN') ?></th>
                            <th><?= __('QT DIA PAGTO COTA') ?></th>
                            <th><?= __('QT DIA PAGTO RESGATE') ?></th>
                            <th><?= __('PR COTA ETF MAX') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($cnpjFundo->doc_extratos_fundos as $docExtratosFundos) : ?>
                        <tr>
                            <td><?= h($docExtratosFundos->cnpj_fundo_id) ?></td>
                            <td><?= h($docExtratosFundos->DT_COMPTC) ?></td>
                            <td><?= h($docExtratosFundos->tipo_anbima_classe_id) ?></td>
                            <td><?= h($docExtratosFundos->APLIC_MIN) ?></td>
                            <td><?= h($docExtratosFundos->QT_DIA_PAGTO_COTA) ?></td>
                            <td><?= h($docExtratosFundos->QT_DIA_PAGTO_RESGATE) ?></td>
                            <td><?= h($docExtratosFundos->PR_COTA_ETF_MAX) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'DocExtratosFundos', 'action' => 'view', $docExtratosFundos->cnpj_fundo_id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'DocExtratosFundos', 'action' => 'edit', $docExtratosFundos->cnpj_fundo_id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'DocExtratosFundos', 'action' => 'delete', $docExtratosFundos->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $docExtratosFundos->cnpj_fundo_id)]) ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    </table>
                </div>
                <?php endif; ?>
            </div>
            <div class="related">
                <h4><?= __('Related Doc Inf Diario Fundos') ?></h4>
                <?php if (!empty($cnpjFundo->doc_inf_diario_fundos)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Cnpj Fundo Id') ?></th>
                            <th><?= __('DT COMPTC') ?></th>
                            <th><?= __('VL TOTAL') ?></th>
                            <th><?= __('VL QUOTA') ?></th>
                            <th><?= __('VL PATRIM LIQ') ?></th>
                            <th><?= __('CAPTC DIA') ?></th>
                            <th><?= __('RESG DIA') ?></th>
                            <th><?= __('NR COTST') ?></th>
                            <th><?= __('Rentab Diaria') ?></th>
                            <th><?= __('Volat Diaria') ?></th>
                            <th><?= __('Rentab Acumulada') ?></th>
                            <th><?= __('Drawdown') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($cnpjFundo->doc_inf_diario_fundos as $docInfDiarioFundos) : ?>
                        <tr>
                            <td><?= h($docInfDiarioFundos->cnpj_fundo_id) ?></td>
                            <td><?= h($docInfDiarioFundos->DT_COMPTC) ?></td>
                            <td><?= h($docInfDiarioFundos->VL_TOTAL) ?></td>
                            <td><?= h($docInfDiarioFundos->VL_QUOTA) ?></td>
                            <td><?= h($docInfDiarioFundos->VL_PATRIM_LIQ) ?></td>
                            <td><?= h($docInfDiarioFundos->CAPTC_DIA) ?></td>
                            <td><?= h($docInfDiarioFundos->RESG_DIA) ?></td>
                            <td><?= h($docInfDiarioFundos->NR_COTST) ?></td>
                            <td><?= h($docInfDiarioFundos->rentab_diaria) ?></td>
                            <td><?= h($docInfDiarioFundos->volat_diaria) ?></td>
                            <td><?= h($docInfDiarioFundos->rentab_acumulada) ?></td>
                            <td><?= h($docInfDiarioFundos->drawdown) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'DocInfDiarioFundos', 'action' => 'view', $docInfDiarioFundos->cnpj_fundo_id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'DocInfDiarioFundos', 'action' => 'edit', $docInfDiarioFundos->cnpj_fundo_id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'DocInfDiarioFundos', 'action' => 'delete', $docInfDiarioFundos->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $docInfDiarioFundos->cnpj_fundo_id)]) ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    </table>
                </div>
                <?php endif; ?>
            </div>
            <div class="related">
                <h4><?= __('Related Indicadores Fundos') ?></h4>
                <?php if (!empty($cnpjFundo->indicadores_fundos)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Cnpj Fundo Id') ?></th>
                            <th><?= __('Periodo Meses') ?></th>
                            <th><?= __('Data Final') ?></th>
                            <th><?= __('Rentabilidade') ?></th>
                            <th><?= __('Desvio Padrao') ?></th>
                            <th><?= __('Num Valores') ?></th>
                            <th><?= __('Rentab Min') ?></th>
                            <th><?= __('Rentab Max') ?></th>
                            <th><?= __('Max Drawdown') ?></th>
                            <th><?= __('Tipo Benchmark Id') ?></th>
                            <th><?= __('Meses Acima Bench') ?></th>
                            <th><?= __('Sharpe') ?></th>
                            <th><?= __('Beta') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($cnpjFundo->indicadores_fundos as $indicadoresFundos) : ?>
                        <tr>
                            <td><?= h($indicadoresFundos->cnpj_fundo_id) ?></td>
                            <td><?= h($indicadoresFundos->periodo_meses) ?></td>
                            <td><?= h($indicadoresFundos->data_final) ?></td>
                            <td><?= h($indicadoresFundos->rentabilidade) ?></td>
                            <td><?= h($indicadoresFundos->desvio_padrao) ?></td>
                            <td><?= h($indicadoresFundos->num_valores) ?></td>
                            <td><?= h($indicadoresFundos->rentab_min) ?></td>
                            <td><?= h($indicadoresFundos->rentab_max) ?></td>
                            <td><?= h($indicadoresFundos->max_drawdown) ?></td>
                            <td><?= h($indicadoresFundos->tipo_benchmark_id) ?></td>
                            <td><?= h($indicadoresFundos->meses_acima_bench) ?></td>
                            <td><?= h($indicadoresFundos->sharpe) ?></td>
                            <td><?= h($indicadoresFundos->beta) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'IndicadoresFundos', 'action' => 'view', $indicadoresFundos->cnpj_fundo_id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'IndicadoresFundos', 'action' => 'edit', $indicadoresFundos->cnpj_fundo_id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'IndicadoresFundos', 'action' => 'delete', $indicadoresFundos->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $indicadoresFundos->cnpj_fundo_id)]) ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    </table>
                </div>
                <?php endif; ?>
            </div>
            <div class="related">
                <h4><?= __('Related Operacoes Financeiras') ?></h4>
                <?php if (!empty($cnpjFundo->operacoes_financeiras)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Id') ?></th>
                            <th><?= __('Usuario Id') ?></th>
                            <th><?= __('Cnpj Fundo Id') ?></th>
                            <th><?= __('Distribuidor Fundo Id') ?></th>
                            <th><?= __('Tipo Operacoes Financeira Id') ?></th>
                            <th><?= __('Por Valor') ?></th>
                            <th><?= __('Valor Total') ?></th>
                            <th><?= __('Valor Cota') ?></th>
                            <th><?= __('Quantidade Cotas') ?></th>
                            <th><?= __('Data') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($cnpjFundo->operacoes_financeiras as $operacoesFinanceiras) : ?>
                        <tr>
                            <td><?= h($operacoesFinanceiras->id) ?></td>
                            <td><?= h($operacoesFinanceiras->usuario_id) ?></td>
                            <td><?= h($operacoesFinanceiras->cnpj_fundo_id) ?></td>
                            <td><?= h($operacoesFinanceiras->distribuidor_fundo_id) ?></td>
                            <td><?= h($operacoesFinanceiras->tipo_operacoes_financeira_id) ?></td>
                            <td><?= h($operacoesFinanceiras->por_valor) ?></td>
                            <td><?= h($operacoesFinanceiras->valor_total) ?></td>
                            <td><?= h($operacoesFinanceiras->valor_cota) ?></td>
                            <td><?= h($operacoesFinanceiras->quantidade_cotas) ?></td>
                            <td><?= h($operacoesFinanceiras->data) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'OperacoesFinanceiras', 'action' => 'view', $operacoesFinanceiras->id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'OperacoesFinanceiras', 'action' => 'edit', $operacoesFinanceiras->id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'OperacoesFinanceiras', 'action' => 'delete', $operacoesFinanceiras->id], ['confirm' => __('Are you sure you want to delete # {0}?', $operacoesFinanceiras->id)]) ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    </table>
                </div>
                <?php endif; ?>
            </div>
            <div class="related">
                <h4><?= __('Related Situacao Fundos') ?></h4>
                <?php if (!empty($cnpjFundo->situacao_fundos)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Cnpj Fundo Id') ?></th>
                            <th><?= __('Tipo Situacao Fundo Id') ?></th>
                            <th><?= __('DT INI SIT') ?></th>
                            <th><?= __('DT REG CVM') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($cnpjFundo->situacao_fundos as $situacaoFundos) : ?>
                        <tr>
                            <td><?= h($situacaoFundos->cnpj_fundo_id) ?></td>
                            <td><?= h($situacaoFundos->tipo_situacao_fundo_id) ?></td>
                            <td><?= h($situacaoFundos->DT_INI_SIT) ?></td>
                            <td><?= h($situacaoFundos->DT_REG_CVM) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'SituacaoFundos', 'action' => 'view', $situacaoFundos->cnpj_fundo_id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'SituacaoFundos', 'action' => 'edit', $situacaoFundos->cnpj_fundo_id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'SituacaoFundos', 'action' => 'delete', $situacaoFundos->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $situacaoFundos->cnpj_fundo_id)]) ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    </table>
                </div>
                <?php endif; ?>
            </div>
        </div>
    </div>
</div>
