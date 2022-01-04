<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoBenchmark $tipoBenchmark
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Tipo Benchmark'), ['action' => 'edit', $tipoBenchmark->id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Tipo Benchmark'), ['action' => 'delete', $tipoBenchmark->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoBenchmark->id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Tipo Benchmarks'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Tipo Benchmark'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoBenchmarks view content">
            <h3><?= h($tipoBenchmark->id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Nome') ?></th>
                    <td><?= h($tipoBenchmark->nome) ?></td>
                </tr>
                <tr>
                    <th><?= __('Sigla') ?></th>
                    <td><?= h($tipoBenchmark->sigla) ?></td>
                </tr>
                <tr>
                    <th><?= __('Id') ?></th>
                    <td><?= $this->Number->format($tipoBenchmark->id) ?></td>
                </tr>
            </table>
            <div class="related">
                <h4><?= __('Related Indicadores Fundos') ?></h4>
                <?php if (!empty($tipoBenchmark->indicadores_fundos)) : ?>
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
                        <?php foreach ($tipoBenchmark->indicadores_fundos as $indicadoresFundos) : ?>
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
                <h4><?= __('Related Rel Benchmarks Classe Fundos') ?></h4>
                <?php if (!empty($tipoBenchmark->rel_benchmarks_classe_fundos)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Tipo Benchmark Id') ?></th>
                            <th><?= __('Tipo Classe Fundo Id') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($tipoBenchmark->rel_benchmarks_classe_fundos as $relBenchmarksClasseFundos) : ?>
                        <tr>
                            <td><?= h($relBenchmarksClasseFundos->tipo_benchmark_id) ?></td>
                            <td><?= h($relBenchmarksClasseFundos->tipo_classe_fundo_id) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'RelBenchmarksClasseFundos', 'action' => 'view', $relBenchmarksClasseFundos->tipo_benchmark_id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'RelBenchmarksClasseFundos', 'action' => 'edit', $relBenchmarksClasseFundos->tipo_benchmark_id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'RelBenchmarksClasseFundos', 'action' => 'delete', $relBenchmarksClasseFundos->tipo_benchmark_id], ['confirm' => __('Are you sure you want to delete # {0}?', $relBenchmarksClasseFundos->tipo_benchmark_id)]) ?>
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
