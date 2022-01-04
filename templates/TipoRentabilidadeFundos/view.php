<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoRentabilidadeFundo $tipoRentabilidadeFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Tipo Rentabilidade Fundo'), ['action' => 'edit', $tipoRentabilidadeFundo->id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Tipo Rentabilidade Fundo'), ['action' => 'delete', $tipoRentabilidadeFundo->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoRentabilidadeFundo->id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Tipo Rentabilidade Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Tipo Rentabilidade Fundo'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoRentabilidadeFundos view content">
            <h3><?= h($tipoRentabilidadeFundo->id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Rentabilidade') ?></th>
                    <td><?= h($tipoRentabilidadeFundo->rentabilidade) ?></td>
                </tr>
                <tr>
                    <th><?= __('Id') ?></th>
                    <td><?= $this->Number->format($tipoRentabilidadeFundo->id) ?></td>
                </tr>
            </table>
            <div class="related">
                <h4><?= __('Related Cadastro Fundos') ?></h4>
                <?php if (!empty($tipoRentabilidadeFundo->cadastro_fundos)) : ?>
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
                        <?php foreach ($tipoRentabilidadeFundo->cadastro_fundos as $cadastroFundos) : ?>
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
        </div>
    </div>
</div>
