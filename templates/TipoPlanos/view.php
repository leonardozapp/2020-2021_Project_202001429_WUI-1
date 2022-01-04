<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoPlano $tipoPlano
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Tipo Plano'), ['action' => 'edit', $tipoPlano->id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Tipo Plano'), ['action' => 'delete', $tipoPlano->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoPlano->id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Tipo Planos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Tipo Plano'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoPlanos view content">
            <h3><?= h($tipoPlano->nome) ?></h3>
            <table>
                <tr>
                    <th><?= __('Nome') ?></th>
                    <td><?= h($tipoPlano->nome) ?></td>
                </tr>
                <tr>
                    <th><?= __('Descricao') ?></th>
                    <td><?= h($tipoPlano->descricao) ?></td>
                </tr>
                <tr>
                    <th><?= __('Permissao') ?></th>
                    <td><?= $tipoPlano->has('permissao') ? $this->Html->link($tipoPlano->permissao->id, ['controller' => 'Permissaos', 'action' => 'view', $tipoPlano->permissao->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Id') ?></th>
                    <td><?= $this->Number->format($tipoPlano->id) ?></td>
                </tr>
            </table>
            <div class="related">
                <h4><?= __('Related Usuarios') ?></h4>
                <?php if (!empty($tipoPlano->usuarios)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Id') ?></th>
                            <th><?= __('Cpf') ?></th>
                            <th><?= __('Nome') ?></th>
                            <th><?= __('Email') ?></th>
                            <th><?= __('Dt Nasc') ?></th>
                            <th><?= __('Senha') ?></th>
                            <th><?= __('Dt Reg') ?></th>
                            <th><?= __('Tipo Plano Id') ?></th>
                            <th><?= __('Tipo Etapas Registro Id') ?></th>
                            <th><?= __('Coment') ?></th>
                            <th><?= __('Created') ?></th>
                            <th><?= __('Modified') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($tipoPlano->usuarios as $usuarios) : ?>
                        <tr>
                            <td><?= h($usuarios->id) ?></td>
                            <td><?= h($usuarios->cpf) ?></td>
                            <td><?= h($usuarios->nome) ?></td>
                            <td><?= h($usuarios->email) ?></td>
                            <td><?= h($usuarios->dt_nasc) ?></td>
                            <td><?= h($usuarios->senha) ?></td>
                            <td><?= h($usuarios->dt_reg) ?></td>
                            <td><?= h($usuarios->tipo_plano_id) ?></td>
                            <td><?= h($usuarios->tipo_etapas_registro_id) ?></td>
                            <td><?= h($usuarios->coment) ?></td>
                            <td><?= h($usuarios->created) ?></td>
                            <td><?= h($usuarios->modified) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'Usuarios', 'action' => 'view', $usuarios->id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'Usuarios', 'action' => 'edit', $usuarios->id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'Usuarios', 'action' => 'delete', $usuarios->id], ['confirm' => __('Are you sure you want to delete # {0}?', $usuarios->id)]) ?>
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
